package pg2mysql

import (
	"database/sql"
	"fmt"

	_ "github.com/go-sql-driver/mysql" // importing mysql driver
)

func NewMySQLDB(
	database string,
	username string,
	password string,
	host string,
	port int,
) DB {
	return &mySQLDB{
		dsn:    fmt.Sprintf("%s:%s@(%s:%d)/%s", username, password, host, port, database),
		dbName: database,
	}
}

type mySQLDB struct {
	dsn    string
	db     *sql.DB
	dbName string
}

func (m *mySQLDB) Open() error {
	db, err := sql.Open("mysql", m.dsn)
	if err != nil {
		return err
	}

	m.db = db

	return nil
}

func (m *mySQLDB) Close() error {
	return m.db.Close()
}

func (m *mySQLDB) GetSchemaRows() (*sql.Rows, error) {
	query := `
	SELECT table_name,
				 column_name,
				 data_type,
				 character_maximum_length
	FROM   information_schema.columns
	WHERE  table_schema = ?`
	rows, err := m.db.Query(query, m.dbName)
	if err != nil {
		return nil, err
	}

	return rows, nil
}

func (m *mySQLDB) DB() *sql.DB {
	return m.db
}
