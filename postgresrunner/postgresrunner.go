package postgresrunner

import (
	"bytes"
	"database/sql"
	"fmt"
	"os/exec"

	. "github.com/onsi/gomega"
)

type Runner struct {
	DBName string

	dbConn *sql.DB
}

func (runner *Runner) DB() *sql.DB {
	return runner.dbConn
}

func (runner *Runner) Setup() {
	cmd := exec.Command("createdb", runner.DBName)
	var out bytes.Buffer
	cmd.Stdout = &out
	cmd.Stderr = &out
	err := cmd.Run()
	if err != nil {
		println(out.String())
	}
	Expect(err).NotTo(HaveOccurred())

	dsn := fmt.Sprintf("host=127.0.0.1 port=5432 sslmode=disable dbname=%s", runner.DBName)
	dbConn, err := sql.Open("postgres", dsn)
	Expect(err).NotTo(HaveOccurred())

	runner.dbConn = dbConn
}

func (runner *Runner) Teardown() {
	if runner.dbConn != nil {
		err := runner.dbConn.Close()
		Expect(err).NotTo(HaveOccurred())
	}

	cmd := exec.Command("dropdb", runner.DBName)
	var out bytes.Buffer
	cmd.Stdout = &out
	cmd.Stderr = &out
	err := cmd.Run()
	if err != nil {
		println(out.String())
	}
	Expect(err).NotTo(HaveOccurred())
}

func (runner *Runner) Truncate() {
	stmt := `
	SELECT table_name
	FROM   information_schema.columns
	WHERE  table_schema = 'public'
				 AND table_catalog = $1`

	rows, err := runner.dbConn.Query(stmt, runner.DBName)
	Expect(err).NotTo(HaveOccurred())
	defer rows.Close()

	for rows.Next() {
		var tableName string
		err := rows.Scan(&tableName)
		Expect(err).NotTo(HaveOccurred())

		_, err = runner.dbConn.Exec(fmt.Sprintf(`TRUNCATE TABLE %s`, tableName))
		Expect(err).NotTo(HaveOccurred())
	}

	Expect(rows.Err()).NotTo(HaveOccurred())
}
