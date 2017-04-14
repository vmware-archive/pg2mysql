package pg2mysql

import (
	"database/sql"
	"fmt"
	"strings"
)

type DB interface {
	Open() error
	Close() error
	GetSchemaRows() (*sql.Rows, error)
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
	_, err := t.GetColumn(name)
	return err == nil
}

func (t *Table) GetColumn(name string) (*Column, error) {
	for _, column := range t.Columns {
		if column.Name == name {
			return column, nil
		}
	}

	return nil, fmt.Errorf("column '%s' not found", name)
}

func (t *Table) GetIncompatibleColumns(other *Table) ([]*Column, error) {
	var incompatibleColumns []*Column
	for _, column := range t.Columns {
		otherColumn, err := other.GetColumn(column.Name)
		if err != nil {
			return nil, fmt.Errorf("failed to find column '%s/%s' in destination schema: %s", t.Name, column.Name, err)
		}

		if column.Incompatible(otherColumn) {
			incompatibleColumns = append(incompatibleColumns, column)
		}
	}

	return incompatibleColumns, nil
}

func (t *Table) GetIncompatibleRowIDs(db DB, columns []*Column) ([]int, error) {
	var limits []string
	for _, column := range columns {
		limits = append(limits, fmt.Sprintf("LENGTH(%s) > %d", column.Name, column.MaxChars))
	}

	stmt := fmt.Sprintf("SELECT id FROM %s WHERE %s", t.Name, strings.Join(limits, " OR "))
	rows, err := db.DB().Query(stmt)
	if err != nil {
		return nil, fmt.Errorf("failed getting incompatible row ids: %s", err)
	}
	defer rows.Close()

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

	return rowIDs, nil
}

func (t *Table) GetIncompatibleRowCount(db DB, columns []*Column) (int64, error) {
	var limits []string
	for _, column := range columns {
		limits = append(limits, fmt.Sprintf("length(%s) > %d", column.Name, column.MaxChars))
	}

	stmt := fmt.Sprintf("SELECT count(1) FROM %s WHERE %s", t.Name, strings.Join(limits, " OR "))

	var count int64
	err := db.DB().QueryRow(stmt).Scan(&count)
	if err != nil {
		return 0, err
	}

	return count, nil
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
	defer rows.Close()

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
