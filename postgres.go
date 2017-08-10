package pg2mysql

import (
	"database/sql"
	"fmt"

	_ "github.com/lib/pq" // register postgres driver
)

func NewPostgreSQLDB(
	database string,
	username string,
	password string,
	host string,
	port int,
	sslMode string,
) DB {
	dsn := fmt.Sprintf("dbname=%s host=%s port=%d sslmode=%s", database, host, port, sslMode)

	if username != "" {
		dsn = fmt.Sprintf("%s user=%s", dsn, username)
	}
	if password != "" {
		dsn = fmt.Sprintf("%s password=%s", dsn, password)
	}

	return &postgreSQLDB{
		dsn:    dsn,
		dbName: database,
	}
}

type postgreSQLDB struct {
	dbName string
	db     *sql.DB
	dsn    string
}

func (p *postgreSQLDB) Open() error {
	db, err := sql.Open("postgres", p.dsn)
	if err != nil {
		return err
	}

	p.db = db

	return nil
}

func (p *postgreSQLDB) Close() error {
	return p.db.Close()
}

func (p *postgreSQLDB) GetSchemaRows() (*sql.Rows, error) {
	stmt := `
	SELECT t1.table_name,
	       t1.column_name,
	       t1.data_type,
	       t1.character_maximum_length
	FROM   information_schema.columns t1
	       JOIN information_schema.tables t2
	         ON t2.table_name = t1.table_name
	            AND t2.table_type = 'BASE TABLE'
	WHERE  t1.table_schema = 'public'
	       AND t1.table_name NOT IN ('schema_migrations')
	       AND t1.table_catalog = $1`

	rows, err := p.db.Query(stmt, p.dbName)
	if err != nil {
		return nil, err
	}

	return rows, nil
}

func (p *postgreSQLDB) DB() *sql.DB {
	return p.db
}

func (p *postgreSQLDB) ColumnNameForSelect(name string) string {
	return name
}

func (p *postgreSQLDB) EnableConstraints() error {
	panic("not implemented")
}

func (p *postgreSQLDB) DisableConstraints() error {
	panic("not implemented")
}
