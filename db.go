package pg2mysql

import (
	"database/sql"
	"fmt"
	"log"
	"strings"
	"time"
)

type DB interface {
	Open() error
	Close() error
	GetSchemaRows() (*sql.Rows, error)
	DisableConstraints() error
	EnableConstraints() error
	ColumnNameForSelect(columnName string) string
	DB() *sql.DB
}

type Schema struct {
	Tables map[string]*Table
}

func (s *Schema) GetTable(name string) (*Table, error) {
	if table, ok := s.Tables[name]; ok {
		return table, nil
	}

	return nil, fmt.Errorf("table '%s' not found", name)
}

type Table struct {
	Name    string
	Columns []*Column
}

func (t *Table) HasColumn(name string) bool {
	_, _, err := t.GetColumn(name)
	return err == nil
}

func (t *Table) GetColumn(name string) (int, *Column, error) {
	for i, column := range t.Columns {
		if column.Name == name {
			return i, column, nil
		}
	}

	return -1, nil, fmt.Errorf("column '%s' not found", name)
}

type Column struct {
	Name     string
	Type     string
	MaxChars int64
}

func (c *Column) Compatible(other *Column) bool {
	if c.MaxChars == 0 && other.MaxChars == 0 {
		return true
	}

	if c.MaxChars > 0 && other.MaxChars > 0 {
		return c.MaxChars < other.MaxChars
	}

	return false
}

func (c *Column) Incompatible(other *Column) bool {
	return !c.Compatible(other)
}

func BuildSchema(db DB) (*Schema, error) {
	rows, err := db.GetSchemaRows()
	if err != nil {
		return nil, err
	}

	data := map[string][]*Column{}
	for rows.Next() {
		var (
			table    sql.NullString
			column   sql.NullString
			datatype sql.NullString
			maxChars sql.NullInt64
		)

		if err := rows.Scan(&table, &column, &datatype, &maxChars); err != nil {
			return nil, err
		}

		data[table.String] = append(data[table.String], &Column{
			Name:     column.String,
			Type:     datatype.String,
			MaxChars: maxChars.Int64,
		})
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("failed to iterate through schema rows: %s", err)
	}

	if err := rows.Close(); err != nil {
		return nil, fmt.Errorf("failed closing rows: %s", err)
	}

	schema := &Schema{
		Tables: map[string]*Table{},
	}

	for k, v := range data {
		schema.Tables[k] = &Table{
			Name:    k,
			Columns: v,
		}
	}

	return schema, nil
}

func GetIncompatibleColumns(src, dst *Table) ([]*Column, error) {
	var incompatibleColumns []*Column
	for _, dstColumn := range dst.Columns {
		_, srcColumn, err := src.GetColumn(dstColumn.Name)
		if err != nil {
			return nil, fmt.Errorf("failed to find column '%s/%s' in source schema: %s", dst.Name, dstColumn.Name, err)
		}

		if dstColumn.Incompatible(srcColumn) {
			incompatibleColumns = append(incompatibleColumns, dstColumn)
		}
	}

	return incompatibleColumns, nil
}

func GetIncompatibleRowIDs(db DB, src, dst *Table) ([]int, error) {
	columns, err := GetIncompatibleColumns(src, dst)
	if err != nil {
		return nil, fmt.Errorf("failed getting incompatible columns: %s", err)
	}

	if columns == nil {
		return nil, nil
	}

	limits := make([]string, len(columns))
	for i, column := range columns {
		limits[i] = fmt.Sprintf("LENGTH(%s) > %d", column.Name, column.MaxChars)
	}

	stmt := fmt.Sprintf("SELECT id FROM %s WHERE %s", src.Name, strings.Join(limits, " OR "))
	rows, err := db.DB().Query(stmt)
	if err != nil {
		return nil, fmt.Errorf("failed getting incompatible row ids: %s", err)
	}

	var rowIDs []int
	for rows.Next() {
		var id int
		if err := rows.Scan(&id); err != nil {
			return nil, fmt.Errorf("failed to scan row: %s", err)
		}
		rowIDs = append(rowIDs, id)
	}

	if err := rows.Err(); err != nil {
		return nil, err
	}

	if err := rows.Close(); err != nil {
		return nil, err
	}

	return rowIDs, nil
}

func GetIncompatibleRowCount(db DB, src, dst *Table) (int64, error) {
	columns, err := GetIncompatibleColumns(src, dst)
	if err != nil {
		return 0, fmt.Errorf("failed getting incompatible columns: %s", err)
	}

	if columns == nil {
		return 0, nil
	}

	limits := make([]string, len(columns))
	for i, column := range columns {
		limits[i] = fmt.Sprintf("length(%s) > %d", column.Name, column.MaxChars)
	}

	stmt := fmt.Sprintf("SELECT count(1) FROM %s WHERE %s", src.Name, strings.Join(limits, " OR "))

	var count int64
	err = db.DB().QueryRow(stmt).Scan(&count)
	if err != nil {
		return 0, err
	}

	return count, nil
}

func EachMissingRow(src, dst DB, table *Table, f func([]interface{})) error {
	srcColumnNamesForSelect := make([]string, len(table.Columns))
	values := make([]interface{}, len(table.Columns))
	scanArgs := make([]interface{}, len(table.Columns))
	colVals := make([]string, len(table.Columns))
	for i := range table.Columns {
		srcColumnNamesForSelect[i] = src.ColumnNameForSelect(table.Columns[i].Name)
		scanArgs[i] = &values[i]
		colVals[i] = fmt.Sprintf("%s <=> ?", dst.ColumnNameForSelect(table.Columns[i].Name))
	}

	// select all rows in src
	stmt := fmt.Sprintf("SELECT %s FROM %s", strings.Join(srcColumnNamesForSelect, ","), table.Name)
	rows, err := src.DB().Query(stmt)
	if err != nil {
		return fmt.Errorf("failed to select rows: %s", err)
	}

	stmt = fmt.Sprintf(`SELECT EXISTS (SELECT 1 FROM %s WHERE %s)`, table.Name, strings.Join(colVals, " AND "))
	preparedStmt, err := dst.DB().Prepare(stmt)
	if err != nil {
		return fmt.Errorf("failed to prepare statement: %s", err)
	}

	var exists bool
	for rows.Next() {
		if err = rows.Scan(scanArgs...); err != nil {
			return fmt.Errorf("failed to scan row: %s", err)
		}

		for i := range scanArgs {
			arg := scanArgs[i]
			iface, ok := arg.(*interface{})
			if !ok {
				log.Fatalf("received unexpected type as scanArg: %T (should be *interface{})", arg)
			}

			// replace the precise PostgreSQL time with a less precise MySQL-compatible time
			if t1, ok := (*iface).(time.Time); ok {
				var timeArg interface{} = t1.Truncate(time.Second)
				scanArgs[i] = &timeArg
			}
		}

		// determine if the row exists in dst
		if err = preparedStmt.QueryRow(scanArgs...).Scan(&exists); err != nil {
			return fmt.Errorf("failed to check if row exists: %s", err)
		}

		if !exists {
			f(scanArgs)
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
