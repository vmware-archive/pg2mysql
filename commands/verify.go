package commands

import (
	"fmt"

	"github.com/pivotal-cf/pg2mysql"
)

type VerifyCommand struct{}

func (c *VerifyCommand) Execute([]string) error {
	mysql := pg2mysql.NewMySQLDB(
		PG2MySQL.Config.MySQL.Database,
		PG2MySQL.Config.MySQL.Username,
		PG2MySQL.Config.MySQL.Password,
		PG2MySQL.Config.MySQL.Host,
		PG2MySQL.Config.MySQL.Port,
		PG2MySQL.Config.MySQL.Charset,
		PG2MySQL.Config.MySQL.Collation,
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

	watcher := pg2mysql.NewStdoutPrinter()
	err = pg2mysql.NewVerifier(pg, mysql, watcher).Verify()
	if err != nil {
		return fmt.Errorf("failed to verify: %s", err)
	}

	return nil
}
