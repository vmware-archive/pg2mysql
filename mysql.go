package pg2mysql

import (
	"database/sql"
	"errors"
	"fmt"
	"strings"

	"github.com/go-sql-driver/mysql"
)

var mysqlTimestampFormat = "2006-01-02 15:04:05"

func NewMySQLDB(
	database string,
	username string,
	password string,
	host string,
	port int,
) DB {
	config := mysql.Config{
		User:            username,
		Passwd:          password,
		DBName:          database,
		Net:             "tcp",
		Addr:            fmt.Sprintf("%s:%d", host, port),
		MultiStatements: true,
		Params: map[string]string{
			"charset":   "utf8",
			"parseTime": "True",
		},
	}

	return &mySQLDB{
		dsn:    config.FormatDSN(),
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

func (m *mySQLDB) EnableConstraints() error {
	_, err := m.db.Exec("SET FOREIGN_KEY_CHECKS = 1;")
	return err
}

func (m *mySQLDB) DisableConstraints() error {
	_, err := m.db.Exec("SET FOREIGN_KEY_CHECKS = 0;")
	return err
}

func (m *mySQLDB) Insert(tableName string, columns []string, values []interface{}) error {
	placeholders := make([]string, len(values))
	for i := range values {
		placeholders[i] = "?"
	}

	stmt := fmt.Sprintf(
		"INSERT INTO %s (%s) VALUES (%s)",
		tableName,
		strings.Join(columns, ","),
		strings.Join(placeholders, ","),
	)
	result, err := m.db.Exec(stmt, values...)
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
