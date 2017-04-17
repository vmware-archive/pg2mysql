package pg2mysql

import (
	"fmt"
	"log"
	"os"
	"strings"
	"time"
)

type Migrator interface {
	Migrate() error
}

func NewMigrator(src, dst DB, truncateFirst bool) Migrator {
	return &migrator{
		src:           src,
		dst:           dst,
		truncateFirst: truncateFirst,
	}
}

type migrator struct {
	src, dst      DB
	truncateFirst bool
}

func (m *migrator) Migrate() error {
	srcSchema, err := BuildSchema(m.src)
	if err != nil {
		return fmt.Errorf("failed to build source schema: %s", err)
	}

	err = m.dst.DisableConstraints()
	if err != nil {
		return fmt.Errorf("failed to disable constraints: %s", err)
	}

	defer func() {
		err = m.dst.EnableConstraints()
		if err != nil {
			fmt.Fprintf(os.Stderr, "failed to enable constraints: %s", err)
		}
	}()

	for _, table := range srcSchema.Tables {
		if m.truncateFirst {
			_, err := m.dst.DB().Exec(fmt.Sprintf("TRUNCATE TABLE %s", table.Name))
			if err != nil {
				return fmt.Errorf("failed truncating: %s", err)
			}
		}

		var columnNamesForSelect []string
		var columnNamesForInsert []string
		for i := range table.Columns {
			columnNamesForSelect = append(columnNamesForSelect, table.Columns[i].Name)
			columnNamesForInsert = append(columnNamesForInsert, fmt.Sprintf("`%s`", table.Columns[i].Name))
		}

		// We don't know how many columns there are or what the types are, so we
		// need to give db.Scan a *interface{} for each column. Later we unpack
		// what's actually in each *interface{} with scanArgToInsertVal.
		values := make([]interface{}, len(columnNamesForSelect))
		scanArgs := make([]interface{}, len(values))
		for i := range values {
			scanArgs[i] = &values[i]
		}

		var recordsInserted int64

		if table.HasColumn("id") {
			err := migrateWithIDs(columnNamesForSelect, columnNamesForInsert, m.src, m.dst, table, scanArgs, &recordsInserted)
			if err != nil {
				return fmt.Errorf("failed migrating table with ids: %s", err)
			}
		} else {
			err := migrateWithoutIDs(columnNamesForSelect, columnNamesForInsert, m.src, m.dst, table, scanArgs, &recordsInserted)
			if err != nil {
				return fmt.Errorf("failed migrating table without ids: %s", err)
			}
		}

		fmt.Printf("inserted %d records into %s\n", recordsInserted, table.Name)
	}

	return nil
}

func scanArgToInsertVal(column *Column, scanArg interface{}) string {
	ifacePtr, ok := scanArg.(*interface{})
	if !ok {
		log.Fatalf("received value which is not pointer to interface: %#v", scanArg)
	}

	iface := *ifacePtr
	switch val := iface.(type) {
	case nil:
		return "NULL"
	case int, int32, int64:
		return fmt.Sprintf("%d", val)
	case bool:
		return fmt.Sprintf("%t", val)
	case string:
		return fmt.Sprintf("'%s'", val)
	case []byte:
		if column.Type == "USER-DEFINED" { // citext is USER-DEFINED
			return fmt.Sprintf("'%s'", val)
		}
		return fmt.Sprintf("%s", val)
	case time.Time:
		timeStr := val.String()
		if matched := postgresTimestampRegexp.MatchString(timeStr); matched {
			timestampWithoutTimeZone := strings.Replace(timeStr, " +0000", "", -1)
			t, err := time.Parse(mysqlTimestampFormat, timestampWithoutTimeZone)
			if err != nil {
				log.Fatalf("failed parsing time '%s': %s", timestampWithoutTimeZone, err)
			}
			return fmt.Sprintf("'%s'", t.Format(mysqlTimestampFormat))
		}
		log.Fatalf("'%s' looks like a timestamp but was not matched", val)
	}

	panic(fmt.Sprintf("don't know how to convert scan arg to insert val for %T", *ifacePtr))
}

func migrateWithIDs(
	columnNamesForSelect []string,
	columnNamesForInsert []string,
	src DB,
	dst DB,
	table *Table,
	scanArgs []interface{},
	recordsInserted *int64,
) error {
	// find ids already in dst
	rows, err := dst.DB().Query(fmt.Sprintf("SELECT id FROM %s", table.Name))
	if err != nil {
		return fmt.Errorf("failed to select id from rows: %s", err)
	}

	var dstIDs []string
	for rows.Next() {
		var id string
		if err = rows.Scan(&id); err != nil {
			return fmt.Errorf("failed to scan id from row: %s", err)
		}
		dstIDs = append(dstIDs, id)
	}

	if err = rows.Err(); err != nil {
		return fmt.Errorf("failed iterating through rows: %s", err)
	}

	if err = rows.Close(); err != nil {
		return fmt.Errorf("failed closing rows: %s", err)
	}

	// select data for ids to migrate from src
	stmt := fmt.Sprintf(
		"SELECT %s FROM %s",
		strings.Join(columnNamesForSelect, ","),
		table.Name,
	)

	if len(dstIDs) > 0 {
		stmt = fmt.Sprintf("%s WHERE id NOT IN (%s)", stmt, strings.Join(dstIDs, ","))
	}

	rows, err = src.DB().Query(stmt)
	if err != nil {
		return fmt.Errorf("failed to select rows: %s", err)
	}

	for rows.Next() {
		if err = rows.Scan(scanArgs...); err != nil {
			return fmt.Errorf("failed to scan row: %s", err)
		}

		var insertVals []string
		for i, scanArg := range scanArgs {
			insertVals = append(insertVals, scanArgToInsertVal(table.Columns[i], scanArg))
		}

		// insert missing data into dst
		stmt = fmt.Sprintf(
			"INSERT INTO %s (%s) VALUES (%s)",
			table.Name,
			strings.Join(columnNamesForInsert, ","),
			strings.Join(insertVals, ","),
		)
		result, err := dst.DB().Exec(stmt)
		if err != nil {
			fmt.Fprintf(os.Stderr, "failed to insert into %s: %s\n", table.Name, err)
			continue
		}

		rowsAffected, err := result.RowsAffected()
		if err != nil {
			fmt.Fprintf(os.Stderr, "error getting affected rows: %s", err)
		}

		if rowsAffected == 0 {
			return fmt.Errorf("failed to insert row")
		}

		*recordsInserted += rowsAffected
	}

	if err = rows.Err(); err != nil {
		return fmt.Errorf("failed iterating through rows: %s", err)
	}

	if err = rows.Close(); err != nil {
		return fmt.Errorf("failed closing rows: %s", err)
	}

	return nil
}

func migrateWithoutIDs(
	columnNamesForSelect []string,
	columnNamesForInsert []string,
	src DB,
	dst DB,
	table *Table,
	scanArgs []interface{},
	recordsInserted *int64,
) error {
	// select all rows in src
	stmt := fmt.Sprintf("SELECT %s FROM %s", strings.Join(columnNamesForSelect, ","), table.Name)
	rows, err := src.DB().Query(stmt)
	if err != nil {
		return fmt.Errorf("failed to select rows: %s", err)
	}

	for rows.Next() {
		if err = rows.Scan(scanArgs...); err != nil {
			return fmt.Errorf("failed to scan row: %s", err)
		}

		var insertVals []string
		for i, scanArg := range scanArgs {
			insertVals = append(insertVals, scanArgToInsertVal(table.Columns[i], scanArg))
		}

		var colVals []string
		for i := range table.Columns {
			colVals = append(colVals, fmt.Sprintf("%s=%s", table.Columns[i].Name, insertVals[i]))
		}

		// determine if the row exists in dst
		stmt = fmt.Sprintf(`SELECT EXISTS (SELECT 1 FROM %s WHERE %s)`, table.Name, strings.Join(colVals, " AND "))
		var existsInMySQL bool
		if err := dst.DB().QueryRow(stmt).Scan(&existsInMySQL); err != nil {
			return fmt.Errorf("failed to check if row exists: %s", err)
		}

		// insert missing data into dst
		if !existsInMySQL {
			stmt = fmt.Sprintf(
				"INSERT INTO %s (%s) VALUES (%s)",
				table.Name,
				strings.Join(columnNamesForInsert, ","),
				strings.Join(insertVals, ","),
			)
			result, err := dst.DB().Exec(stmt)
			if err != nil {
				fmt.Fprintf(os.Stderr, "failed to insert into %s: %s\n", table.Name, err)
				continue
			}

			rowsAffected, err := result.RowsAffected()
			if err != nil {
				fmt.Fprintf(os.Stderr, "error getting affected rows: %s", err)
			}

			if rowsAffected == 0 {
				return fmt.Errorf("failed to insert row")
			}

			*recordsInserted += rowsAffected
		}
	}

	if err := rows.Err(); err != nil {
		return fmt.Errorf("failed iterating through rows: %s", err)
	}

	if err = rows.Close(); err != nil {
		return fmt.Errorf("failed closing rows: %s", err)
	}

	return nil
}
