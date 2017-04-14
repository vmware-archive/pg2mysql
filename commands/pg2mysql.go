package commands

import (
	"fmt"
	"io/ioutil"

	"github.com/pivotal-cf/pg2mysql"

	yaml "gopkg.in/yaml.v2"
)

type ConfigFilePath string

func (c *ConfigFilePath) UnmarshalFlag(value string) error {
	bs, err := ioutil.ReadFile(value)
	if err != nil {
		return fmt.Errorf("failed to read config: %s", err)
	}

	var config pg2mysql.Config
	err = yaml.Unmarshal(bs, &config)
	if err != nil {
		return fmt.Errorf("failed to unmarshal config: %s", err)
	}

	PG2MySQL.Config = config

	return nil
}

type PG2MySQLCommand struct {
	Config pg2mysql.Config

	ConfigFile ConfigFilePath `short:"c" long:"config" required:"true" description:"Path to config file"`

	Validate ValidateCommand `command:"validate" description:"Validate that the data in PostgreSQL can be migrated to MySQL"`
}

var PG2MySQL PG2MySQLCommand
