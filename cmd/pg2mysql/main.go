package main

import (
	"log"

	flags "github.com/jessevdk/go-flags"
	"github.com/pivotal-cf/pg2mysql/commands"
)

func main() {
	parser := flags.NewParser(&commands.PG2MySQL, flags.HelpFlag)
	parser.NamespaceDelimiter = "-"

	_, err := parser.Parse()
	if err != nil {
		log.Fatalf("error: %s", err)
	}
}
