package pg2mysql

import (
	"database/sql"
	"errors"
	"fmt"
	"os"
	"strings"
)

type Migrator interface {
	Migrate() error
}

func NewMigrator(src, dst DB, truncateFirst bool, watcher MigratorWatcher) Migrator {
	return &migrator{
		src:           src,
		dst:           dst,
		truncateFirst: truncateFirst,
		watcher:       watcher,
	}
}

type migrator struct {
	src, dst      DB
	truncateFirst bool
	watcher       MigratorWatcher
}

func (m *migrator) Migrate() error {
	srcSchema, err := BuildSchema(m.src)
	if err != nil {
		return fmt.Errorf("failed to build source schema: %s", err)
	}

	m.watcher.WillDisableConstraints()
	err = m.dst.DisableConstraints()
	if err != nil {
		return fmt.Errorf("failed to disable constraints: %s", err)
	}
	m.watcher.DidDisableConstraints()

	defer func() {
		m.watcher.WillEnableConstraints()
		err = m.dst.EnableConstraints()
		if err != nil {
			m.watcher.EnableConstraintsDidFailWithError(err)
		} else {
			m.watcher.EnableConstraintsDidFinish()
		}
	}()

	for _, table := range srcSchema.Tables {
		if m.truncateFirst {
			m.watcher.WillTruncateTable(table.Name)
			_, err := m.dst.DB().Exec(fmt.Sprintf("TRUNCATE TABLE %s", table.Name))
			if err != nil {
				return fmt.Errorf("failed truncating: %s", err)
			}
			m.watcher.TruncateTableDidFinish(table.Name)
		}

		columnNamesForInsert := make([]string, len(table.Columns))
		placeholders := make([]string, len(table.Columns))
		for i := range table.Columns {
			columnNamesForInsert[i] = fmt.Sprintf("`%s`", table.Columns[i].Name)
			placeholders[i] = "?"
		}

		preparedStmt, err := m.dst.DB().Prepare(fmt.Sprintf(
			"INSERT INTO %s (%s) VALUES (%s)",
			table.Name,
			strings.Join(columnNamesForInsert, ","),
			strings.Join(placeholders, ","),
		))
		if err != nil {
			return fmt.Errorf("failed creating prepared statement: %s", err)
		}

		var recordsInserted int64

		m.watcher.TableMigrationDidStart(table.Name)

		if table.HasColumn("id") {
			err = migrateWithIDs(m.watcher, m.src, m.dst, table, &recordsInserted, preparedStmt)
			if err != nil {
				return fmt.Errorf("failed migrating table with ids: %s", err)
			}
		} else {
			err = EachMissingRow(m.src, m.dst, table, func(scanArgs []interface{}) {
				err = insert(preparedStmt, scanArgs)
				if err != nil {
					fmt.Fprintf(os.Stderr, "failed to insert into %s: %s\n", table.Name, err)
					return
				}
				recordsInserted++
			})
			if err != nil {
				return fmt.Errorf("failed migrating table without ids: %s", err)
			}
		}

		m.watcher.TableMigrationDidFinish(table.Name, recordsInserted)
	}

	return nil
}

func migrateWithIDs(
	watcher MigratorWatcher,
	src DB,
	dst DB,
	table *Table,
	recordsInserted *int64,
	preparedStmt *sql.Stmt,
) error {
	columnNamesForSelect := make([]string, len(table.Columns))
	values := make([]interface{}, len(table.Columns))
	scanArgs := make([]interface{}, len(table.Columns))
	for i := range table.Columns {
		columnNamesForSelect[i] = table.Columns[i].Name
		scanArgs[i] = &values[i]
	}

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

		err = insert(preparedStmt, scanArgs)
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

func insert(stmt *sql.Stmt, values []interface{}) error {
	result, err := stmt.Exec(values...)
	if err != nil {
		return fmt.Errorf("failed to exec stmt: %s", err)
	}

	rowsAffected, err := result.RowsAffected()
	if err != nil {
		return fmt.Errorf("failed getting rows affected by insert: %s", err)
	}

	if rowsAffected == 0 {
		return errors.New("no rows affected by insert")
	}

	return nil
}
