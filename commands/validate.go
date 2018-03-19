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

	results, err := pg2mysql.NewValidator(pg, mysql).Validate()
	if err != nil {
		return fmt.Errorf("failed to validate: %s", err)
	}

	for _, result := range results {
		switch {
		case len(result.IncompatibleRowIDs) > 0:
			fmt.Printf("found %d incompatible rows in %s with IDs %v\n", result.IncompatibleRowCount, result.TableName, result.IncompatibleRowIDs)

		case result.IncompatibleRowCount > 0:
			fmt.Printf("found %d incompatible rows in %s (which has no 'id' column)\n", result.IncompatibleRowCount, result.TableName)

		default:
			fmt.Printf("%s OK\n", result.TableName)
		}
	}

	return nil
}
