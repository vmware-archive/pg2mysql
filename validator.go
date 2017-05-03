package pg2mysql

import (
	"fmt"
)

type Validator interface {
	Validate() ([]ValidationResult, error)
}

func NewValidator(src, dst DB) Validator {
	return &validator{
		src: src,
		dst: dst,
	}
}

type validator struct {
	src, dst DB
}

func (v *validator) Validate() ([]ValidationResult, error) {
	srcSchema, err := BuildSchema(v.src)
	if err != nil {
		return nil, fmt.Errorf("failed to build source schema: %s", err)
	}

	dstSchema, err := BuildSchema(v.dst)
	if err != nil {
		return nil, fmt.Errorf("failed to build destination schema: %s", err)
	}

	var results []ValidationResult
	for _, srcTable := range srcSchema.Tables {
		dstTable, err := dstSchema.GetTable(srcTable.Name)
		if err != nil {
			return nil, fmt.Errorf("failed to get table from destination schema: %s", err)
		}

		if srcTable.HasColumn("id") {
			rowIDs, err := GetIncompatibleRowIDs(v.src, srcTable, dstTable)
			if err != nil {
				return nil, fmt.Errorf("failed getting incompatible row ids: %s", err)
			}

			results = append(results, ValidationResult{
				TableName:            srcTable.Name,
				IncompatibleRowIDs:   rowIDs,
				IncompatibleRowCount: int64(len(rowIDs)),
			})
		} else {
			rowCount, err := GetIncompatibleRowCount(v.src, srcTable, dstTable)
			if err != nil {
				return nil, fmt.Errorf("failed getting incompatible row count: %s", err)
			}

			results = append(results, ValidationResult{
				TableName:            srcTable.Name,
				IncompatibleRowCount: rowCount,
			})
		}
	}

	return results, nil
}

type ValidationResult struct {
	TableName            string
	IncompatibleRowIDs   []int
	IncompatibleRowCount int64
}
