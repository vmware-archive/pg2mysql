package pg2mysql

import (
	"fmt"
	"os"
	"strings"
)

type Migrator interface {
	Migrate() ([]MigrationResult, error)
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

func (m *migrator) Migrate() ([]MigrationResult, error) {
	srcSchema, err := BuildSchema(m.src)
	if err != nil {
		return nil, fmt.Errorf("failed to build source schema: %s", err)
	}

	err = m.dst.DisableConstraints()
	if err != nil {
		return nil, fmt.Errorf("failed to disable constraints: %s", err)
	}

	defer func() {
		err = m.dst.EnableConstraints()
		if err != nil {
			fmt.Fprintf(os.Stderr, "failed to enable constraints: %s", err)
		}
	}()

	var result []MigrationResult

	for _, table := range srcSchema.Tables {
		if m.truncateFirst {
			_, err := m.dst.DB().Exec(fmt.Sprintf("TRUNCATE TABLE %s", table.Name))
			if err != nil {
				return nil, fmt.Errorf("failed truncating: %s", err)
			}
		}

		columnNamesForSelect := make([]string, len(table.Columns))
		columnNamesForInsert := make([]string, len(table.Columns))
		for i := range table.Columns {
			columnNamesForSelect[i] = table.Columns[i].Name
			columnNamesForInsert[i] = fmt.Sprintf("`%s`", table.Columns[i].Name)
		}

		// We don't know how many columns there are or what the types are, so we
		// need to give db.Scan a *interface{} for each column.
		values := make([]interface{}, len(columnNamesForSelect))
		scanArgs := make([]interface{}, len(values))
		for i := range values {
			scanArgs[i] = &values[i]
		}

		var recordsInserted int64

		if table.HasColumn("id") {
			err := migrateWithIDs(columnNamesForSelect, columnNamesForInsert, m.src, m.dst, table, scanArgs, &recordsInserted)
			if err != nil {
				return nil, fmt.Errorf("failed migrating table with ids: %s", err)
			}
		} else {
			err := migrateWithoutIDs(columnNamesForSelect, columnNamesForInsert, m.src, m.dst, table, scanArgs, &recordsInserted)
			if err != nil {
				return nil, fmt.Errorf("failed migrating table without ids: %s", err)
			}
		}

		if recordsInserted > 0 {
			result = append(result, MigrationResult{
				TableName:    table.Name,
				RowsMigrated: recordsInserted,
			})
		}

		fmt.Printf("inserted %d records into %s\n", recordsInserted, table.Name)
	}

	return result, nil
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

		err = dst.Insert(table.Name, columnNamesForInsert, scanArgs)
		if err != nil {
			fmt.Fprintf(os.Stderr, "failed to insert into %s: %s\n", table.Name, err)
			continue
		}

		*recordsInserted++
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

		colVals := make([]string, len(table.Columns))
		for i := range table.Columns {
			colVals[i] = fmt.Sprintf("%s=?", table.Columns[i].Name)
		}

		// determine if the row exists in dst
		stmt = fmt.Sprintf(`SELECT EXISTS (SELECT 1 FROM %s WHERE %s)`, table.Name, strings.Join(colVals, " AND "))
		var existsInMySQL bool
		if err = dst.DB().QueryRow(stmt, scanArgs...).Scan(&existsInMySQL); err != nil {
			return fmt.Errorf("failed to check if row exists: %s", err)
		}

		// insert missing data into dst
		if !existsInMySQL {
			err = dst.Insert(table.Name, columnNamesForInsert, scanArgs)
			if err != nil {
				fmt.Fprintf(os.Stderr, "failed to insert into %s: %s\n", table.Name, err)
				continue
			}

			*recordsInserted++
		}
	}

	if err = rows.Err(); err != nil {
		return fmt.Errorf("failed iterating through rows: %s", err)
	}

	if err = rows.Close(); err != nil {
		return fmt.Errorf("failed closing rows: %s", err)
	}

	return nil
}

type MigrationResult struct {
	TableName    string
	RowsMigrated int64
	RowsSkipped  int64
}
