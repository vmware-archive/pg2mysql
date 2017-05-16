package pg2mysql

import "fmt"

//go:generate counterfeiter . VerifierWatcher

type VerifierWatcher interface {
	TableVerificationDidStart(tableName string)
	TableVerificationDidFinish(tableName string, missingRows int64)
	TableVerificationDidFinishWithError(tableName string, err error)
}

func NewStdoutPrinter() *StdoutPrinter {
	return &StdoutPrinter{}
}

type StdoutPrinter struct{}

func (s *StdoutPrinter) TableVerificationDidStart(tableName string) {
	fmt.Printf("Verifying table %s...", tableName)
}

func (s *StdoutPrinter) TableVerificationDidFinish(tableName string, missingRows int64) {
	if missingRows != 0 {
		if missingRows == 1 {
			fmt.Println("\n\tFAILED: 1 row missing")
		} else {
			fmt.Printf("\n\tFAILED: %d rows missing\n", missingRows)
		}
	} else {
		s.done()
	}
}

func (s *StdoutPrinter) done() {
	fmt.Println("OK")
}

func (s *StdoutPrinter) TableVerificationDidFinishWithError(tableName string, err error) {
	fmt.Printf("failed: %s", err)
}
