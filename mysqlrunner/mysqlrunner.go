package mysqlrunner

import (
	"database/sql"
	"fmt"

	"github.com/go-sql-driver/mysql"
)

type Runner struct {
	DBName string
	dbConn *sql.DB
}

func (runner *Runner) DB() *sql.DB {
	return runner.dbConn
}

func (runner *Runner) Setup() error {
	dbConfig := &mysql.Config{
		User:            "root",
		Net:             "tcp",
		Addr:            "127.0.0.1:3306",
		MultiStatements: true,
		Params: map[string]string{
			"charset":   "utf8",
			"parseTime": "True",
		},
	}
	dbConn, err := sql.Open("mysql", dbConfig.FormatDSN())
	if err != nil {
		return err
	}

	_, err = dbConn.Exec(fmt.Sprintf("CREATE DATABASE %s", runner.DBName))
	if err != nil {
		return err
	}

	err = dbConn.Close()
	if err != nil {
		return err
	}

	dbConfig.DBName = runner.DBName

	dbConn, err = sql.Open("mysql", dbConfig.FormatDSN())
	if err != nil {
		return err
	}

	runner.dbConn = dbConn

	return nil
}

func (runner *Runner) Teardown() error {
	_, err := runner.dbConn.Exec(fmt.Sprintf("DROP DATABASE %s", runner.DBName))
	if err != nil {
		return err
	}

	err = runner.dbConn.Close()
	if err != nil {
		return err
	}

	return nil
}

func (runner *Runner) Truncate() error {
	rows, err := runner.dbConn.Query(`
		SELECT TABLE_NAME
		FROM INFORMATION_SCHEMA.TABLES
		WHERE TABLE_SCHEMA IN (?)`, runner.DBName,
	)
	if err != nil {
		return err
	}

	for rows.Next() {
		var tableName string
		err := rows.Scan(&tableName)
		if err != nil {
			return err
		}

		_, err = runner.dbConn.Exec(fmt.Sprintf(`
			SET FOREIGN_KEY_CHECKS = 0;
			TRUNCATE TABLE %s.%s;
			SET FOREIGN_KEY_CHECKS = 1`,
			runner.DBName, tableName))
		if err != nil {
			return err
		}
	}

	if err := rows.Err(); err != nil {
		return err
	}

	return rows.Close()
}
