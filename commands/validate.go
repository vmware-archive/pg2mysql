package commands

import (
	"fmt"

	"github.com/pivotal-cf/pg2mysql"
)

type ValidateCommand struct{}

func (c *ValidateCommand) Execute([]string) error {
	mysql := pg2mysql.NewMySQLDB(
		PG2MySQL.Config.MySQL.Database,
		PG2MySQL.Config.MySQL.Username,
		PG2MySQL.Config.MySQL.Password,
		PG2MySQL.Config.MySQL.Host,
		PG2MySQL.Config.MySQL.Port,
	)

	err := mysql.Open()
	if err != nil {
		return fmt.Errorf("failed to open mysql connection: %s", err)
	}
	defer mysql.Close()

	mysqlSchema, err := pg2mysql.BuildSchema(mysql)
	if err != nil {
		return fmt.Errorf("failed to build mysql schema: %s", err)
	}

	pg := pg2mysql.NewPostgreSQLDB(
		PG2MySQL.Config.PostgreSQL.Database,
		PG2MySQL.Config.PostgreSQL.Username,
		PG2MySQL.Config.PostgreSQL.Password,
		PG2MySQL.Config.PostgreSQL.Host,
		PG2MySQL.Config.PostgreSQL.Port,
		PG2MySQL.Config.PostgreSQL.SSLMode,
	)
	err = pg.Open()
	if err != nil {
		return fmt.Errorf("failed to open pg connection: %s", err)
	}
	defer pg.Close()

	pgSchema, err := pg2mysql.BuildSchema(pg)
	if err != nil {
		return fmt.Errorf("failed to build postgres schema: %s", err)
	}

	for _, pgTable := range pgSchema.Tables {
		mysqlTable, err := mysqlSchema.GetTable(pgTable.Name)
		if err != nil {
			return fmt.Errorf("failed to get table from mysql schema: %s", err)
		}

		if pgTable.HasColumn("id") {
			rowIDs, err := pg2mysql.GetIncompatibleRowIDs(pg, pgTable, mysqlTable)
			if err != nil {
				return fmt.Errorf("failed getting incompatible row ids: %s", err)
			}

			if len(rowIDs) > 0 {
				fmt.Printf("found incompatible rows in %s with IDs %v\n", pgTable.Name, rowIDs)
			}
		} else {
			rowCount, err := pg2mysql.GetIncompatibleRowCount(pg, pgTable, mysqlTable)
			if err != nil {
				return fmt.Errorf("failed getting incompatible row count: %s", err)
			}

			if rowCount > 0 {
				fmt.Printf("found %d incompatible rows in %s (which has no 'id' column)\n", rowCount, pgTable.Name)
			}
		}
	}

	return nil
}
