package pg2mysql_test

import (
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	"github.com/pivotal-cf/pg2mysql"
)

var _ = Describe("Validator", func() {
	var (
		validator pg2mysql.Validator
		mysql     pg2mysql.DB
		pg        pg2mysql.DB
	)

	BeforeEach(func() {
		mysql = pg2mysql.NewMySQLDB(
			mysqlRunner.DBName,
			"root",
			"",
			"127.0.0.1",
			3306,
		)

		err := mysql.Open()
		Expect(err).NotTo(HaveOccurred())

		pg = pg2mysql.NewPostgreSQLDB(
			pgRunner.DBName,
			"",
			"",
			"127.0.0.1",
			5432,
			"disable",
		)
		err = pg.Open()
		Expect(err).NotTo(HaveOccurred())

		validator = pg2mysql.NewValidator(pg, mysql)
	})

	AfterEach(func() {
		err := mysql.Close()
		Expect(err).NotTo(HaveOccurred())
		err = pg.Close()
		Expect(err).NotTo(HaveOccurred())
	})

	Describe("Validate", func() {
		It("returns a result", func() {
			result, err := validator.Validate()
			Expect(err).NotTo(HaveOccurred())
			Expect(result).To(HaveLen(2))
			Expect(result).To(ContainElement(pg2mysql.ValidationResult{
				TableName: "table_with_id",
			}))

			Expect(result).To(ContainElement(pg2mysql.ValidationResult{
				TableName: "table_without_id",
			}))
		})

		Context("when there is compatible data in postgres", func() {
			BeforeEach(func() {
				result, err := pgRunner.DB().Exec("INSERT INTO table_with_id (id, name, ci_name, created_at, truthiness) VALUES (3, 'some-name', 'some-ci-name', now(), false);")
				Expect(err).NotTo(HaveOccurred())
				rowsAffected, err := result.RowsAffected()
				Expect(err).NotTo(HaveOccurred())
				Expect(rowsAffected).To(BeNumerically("==", 1))
			})

			It("returns a result", func() {
				result, err := validator.Validate()
				Expect(err).NotTo(HaveOccurred())
				Expect(result).To(HaveLen(2))
				Expect(result).To(ContainElement(pg2mysql.ValidationResult{
					TableName: "table_with_id",
				}))

				Expect(result).To(ContainElement(pg2mysql.ValidationResult{
					TableName: "table_without_id",
				}))
			})
		})

		Context("when there is incompatible data in postgres in a table with an 'id' column", func() {
			BeforeEach(func() {
				result, err := pgRunner.DB().Exec("INSERT INTO table_with_id (id, name, ci_name, created_at, truthiness) VALUES (3, 'some-name-that-is-too-long-for-mysql-xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx', 'some-other-ci-name', now(), false);")
				Expect(err).NotTo(HaveOccurred())
				rowsAffected, err := result.RowsAffected()
				Expect(err).NotTo(HaveOccurred())
				Expect(rowsAffected).To(BeNumerically("==", 1))
			})

			It("returns a result", func() {
				result, err := validator.Validate()
				Expect(err).NotTo(HaveOccurred())

				Expect(result).To(HaveLen(2))
				Expect(result).To(ContainElement(pg2mysql.ValidationResult{
					TableName:            "table_with_id",
					IncompatibleRowIDs:   []int{3},
					IncompatibleRowCount: 1,
				}))

				Expect(result).To(ContainElement(pg2mysql.ValidationResult{
					TableName: "table_without_id",
				}))
			})
		})

		Context("when there is incompatible data in postgres in a table without an 'id' column", func() {
			BeforeEach(func() {
				result, err := pgRunner.DB().Exec("INSERT INTO table_without_id (name, ci_name, created_at, truthiness) VALUES ('some-name-that-is-too-long-for-mysql-xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx', 'some-other-ci-name', now(), false);")
				Expect(err).NotTo(HaveOccurred())
				rowsAffected, err := result.RowsAffected()
				Expect(err).NotTo(HaveOccurred())
				Expect(rowsAffected).To(BeNumerically("==", 1))
			})

			It("returns a result", func() {
				result, err := validator.Validate()
				Expect(err).NotTo(HaveOccurred())
				Expect(result).To(HaveLen(2))
				Expect(result).To(ContainElement(pg2mysql.ValidationResult{
					TableName: "table_with_id",
				}))

				Expect(result).To(ContainElement(pg2mysql.ValidationResult{
					TableName:            "table_without_id",
					IncompatibleRowIDs:   nil,
					IncompatibleRowCount: 1,
				}))
			})
		})
	})
})
