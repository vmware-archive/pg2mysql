package pg2mysql

import (
	"fmt"
	"log"
	"strings"
	"time"
)

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
	columnNamesForSelect := make([]string, len(table.Columns))
	columnNamesForInsert := make([]string, len(table.Columns))
	values := make([]interface{}, len(table.Columns))
	scanArgs := make([]interface{}, len(table.Columns))
	colVals := make([]string, len(table.Columns))
	for i := range table.Columns {
		columnNamesForSelect[i] = table.Columns[i].Name
		columnNamesForInsert[i] = fmt.Sprintf("`%s`", table.Columns[i].Name)
		scanArgs[i] = &values[i]
		colVals[i] = fmt.Sprintf("%s <=> ?", table.Columns[i].Name)
	}

	// select all rows in src
	stmt := fmt.Sprintf("SELECT %s FROM %s", strings.Join(columnNamesForSelect, ","), table.Name)
	rows, err := src.DB().Query(stmt)
	if err != nil {
		return VerificationResult{}, fmt.Errorf("failed to select rows: %s", err)
	}

	stmt = fmt.Sprintf(`SELECT EXISTS (SELECT 1 FROM %s WHERE %s)`, table.Name, strings.Join(colVals, " AND "))
	preparedStmt, err := dst.DB().Prepare(stmt)
	if err != nil {
		return VerificationResult{}, fmt.Errorf("failed to prepare statement: %s", err)
	}

	var missingRows int64
	var exists bool
	for rows.Next() {
		if err = rows.Scan(scanArgs...); err != nil {
			return VerificationResult{}, fmt.Errorf("failed to scan row: %s", err)
		}

		for i := range scanArgs {
			arg := scanArgs[i]
			iface, ok := arg.(*interface{})
			if !ok {
				log.Fatalf("received unexpected type as scanArg: %T (should be *interface{})", arg)
			}

			// replace the precise PostgreSQL time with a less precise MySQL-compatible time
			if t1, ok := (*iface).(time.Time); ok {
				scanArgs[i] = t1.Truncate(time.Second)
			}
		}

		// determine if the row exists in dst
		if err = preparedStmt.QueryRow(scanArgs...).Scan(&exists); err != nil {
			return VerificationResult{}, fmt.Errorf("failed to check if row exists: %s", err)
		}

		if !exists {
			missingRows++
		}
	}

	if err = rows.Err(); err != nil {
		return VerificationResult{}, fmt.Errorf("failed iterating through rows: %s", err)
	}

	if err = rows.Close(); err != nil {
		return VerificationResult{}, fmt.Errorf("failed closing rows: %s", err)
	}

	return VerificationResult{
		TableName:       table.Name,
		MissingRowCount: missingRows,
	}, nil
}
