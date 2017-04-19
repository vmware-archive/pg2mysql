package commands

import (
	"fmt"

	"github.com/pivotal-cf/pg2mysql"
)

type MigrateCommand struct {
	Truncate bool `long:"truncate" description:"Truncate destination tables before migrating data"`
}

func (c *MigrateCommand) Execute([]string) error {
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

	_, err = pg2mysql.NewMigrator(pg, mysql, c.Truncate).Migrate()
	if err != nil {
		return fmt.Errorf("failed migrating: %s", err)
	}

	return nil
}
