package pg2mysql

import "fmt"

type Verifier interface {
	Verify() ([]VerificationResult, error)
}

type VerificationResult struct {
	TableName       string
	MissingRowCount int64
}

type verifier struct {
	src, dst DB
}

func NewVerifier(src, dst DB) Verifier {
	return &verifier{
		src: src,
		dst: dst,
	}
}

func (c *verifier) Verify() ([]VerificationResult, error) {
	srcSchema, err := BuildSchema(c.src)
	if err != nil {
		return nil, fmt.Errorf("failed to build source schema: %s", err)
	}

	var results []VerificationResult
	for _, table := range srcSchema.Tables {
		result, err := verifyTable(c.src, c.dst, table)
		if err != nil {
			return nil, err
		}
		results = append(results, result)
	}

	return results, nil
}

func verifyTable(src, dst DB, table *Table) (VerificationResult, error) {
	var missingRows int64
	err := EachMissingRow(src, dst, table, func(scanArgs []interface{}) {
		missingRows++
	})

	if err != nil {
		return VerificationResult{}, fmt.Errorf("failed finding missing rows: %s", err)
	}

	return VerificationResult{
		TableName:       table.Name,
		MissingRowCount: missingRows,
	}, nil
}
