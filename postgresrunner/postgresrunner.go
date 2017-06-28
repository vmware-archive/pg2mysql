package postgresrunner

import (
	"bytes"
	"database/sql"
	"fmt"
	"os/exec"
)

type Runner struct {
	DBName string

	dbConn *sql.DB
}

func (runner *Runner) DB() *sql.DB {
	return runner.dbConn
}

func (runner *Runner) Setup() error {
	cmd := exec.Command("createdb", runner.DBName)
	var out bytes.Buffer
	cmd.Stdout = &out
	cmd.Stderr = &out
	err := cmd.Run()
	if err != nil {
		return err
	}

	dsn := fmt.Sprintf("host=127.0.0.1 port=5432 sslmode=disable dbname=%s", runner.DBName)
	dbConn, err := sql.Open("postgres", dsn)
	if err != nil {
		return err
	}

	runner.dbConn = dbConn

	return nil
}

func (runner *Runner) Teardown() error {
	if runner.dbConn != nil {
		err := runner.dbConn.Close()
		if err != nil {
			return err
		}

		cmd := exec.Command("dropdb", runner.DBName)

		var out bytes.Buffer
		cmd.Stdout = &out
		cmd.Stderr = &out

		return cmd.Run()
	}

	return nil
}

func (runner *Runner) Truncate() error {
	stmt := `
	SELECT table_name
	FROM   information_schema.columns
	WHERE  table_schema = 'public'
				 AND table_catalog = $1`

	rows, err := runner.dbConn.Query(stmt, runner.DBName)
	if err != nil {
		return err
	}

	for rows.Next() {
		var tableName string
		err := rows.Scan(&tableName)
		if err != nil {
			return err
		}

		_, err = runner.dbConn.Exec(fmt.Sprintf(`TRUNCATE TABLE %s`, tableName))
		if err != nil {
			return err
		}
	}

	if err := rows.Err(); err != nil {
		return err
	}

	return rows.Close()
}
