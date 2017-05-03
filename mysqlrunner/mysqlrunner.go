package mysqlrunner

import (
	"database/sql"
	"fmt"

	"github.com/go-sql-driver/mysql"
	. "github.com/onsi/gomega"
)

type Runner struct {
	DBName   string
	dbConn   *sql.DB
	dbConfig *mysql.Config
}

func (runner *Runner) DB() *sql.DB {
	return runner.dbConn
}

func (runner *Runner) Setup() {
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
	Expect(err).NotTo(HaveOccurred())

	_, err = dbConn.Exec(fmt.Sprintf("CREATE DATABASE %s", runner.DBName))
	Expect(err).NotTo(HaveOccurred())

	dbConn.Close()

	dbConfig.DBName = runner.DBName

	dbConn, err = sql.Open("mysql", dbConfig.FormatDSN())
	Expect(err).NotTo(HaveOccurred())

	runner.dbConn = dbConn
}

func (runner *Runner) Teardown() {
	_, err := runner.dbConn.Exec(fmt.Sprintf("DROP DATABASE %s", runner.DBName))
	Expect(err).NotTo(HaveOccurred())

	err = runner.dbConn.Close()
	Expect(err).NotTo(HaveOccurred())
}

func (runner *Runner) Truncate() {
	rows, err := runner.dbConn.Query(`
		SELECT TABLE_NAME
		FROM INFORMATION_SCHEMA.TABLES
		WHERE TABLE_SCHEMA IN (?)`, runner.DBName,
	)
	Expect(err).NotTo(HaveOccurred())
	defer rows.Close()

	for rows.Next() {
		var tableName string
		err := rows.Scan(&tableName)
		Expect(err).NotTo(HaveOccurred())

		_, err = runner.dbConn.Exec(fmt.Sprintf(`
			SET FOREIGN_KEY_CHECKS = 0;
			TRUNCATE TABLE %s.%s;
			SET FOREIGN_KEY_CHECKS = 1`,
			runner.DBName, tableName))
		Expect(err).NotTo(HaveOccurred())
	}
}
