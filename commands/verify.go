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

	results, err := pg2mysql.NewVerifier(pg, mysql).Verify()
	if err != nil {
		return fmt.Errorf("failed to verify: %s", err)
	}

	for _, result := range results {
		if result.MissingRowCount > 0 {
			fmt.Printf("found %d missing rows in %s\n", result.MissingRowCount, result.TableName)
			continue
		}

		fmt.Printf("%s OK\n", result.TableName)
	}

	return nil
}
