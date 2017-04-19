package pg2mysql_test

import (
	"fmt"
	"time"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	"github.com/pivotal-cf/pg2mysql"
)

var _ = FDescribe("Migrator", func() {
	var (
		migrator      pg2mysql.Migrator
		mysql         pg2mysql.DB
		pg            pg2mysql.DB
		truncateFirst bool
	)

	BeforeEach(func() {
		_, err := mysqlRunner.DB().Exec(fmt.Sprintf("USE %s", mysqlRunner.DBName))
		Expect(err).NotTo(HaveOccurred())

		mysql = pg2mysql.NewMySQLDB(
			mysqlRunner.DBName,
			"root",
			"",
			"127.0.0.1",
			3306,
		)

		err = mysql.Open()
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

		migrator = pg2mysql.NewMigrator(pg, mysql, truncateFirst)
	})

	AfterEach(func() {
		err := mysql.Close()
		Expect(err).NotTo(HaveOccurred())
		err = pg.Close()
		Expect(err).NotTo(HaveOccurred())
	})

	Describe("Migrate", func() {
		It("returns an empty result", func() {
			result, err := migrator.Migrate()
			Expect(err).NotTo(HaveOccurred())
			Expect(result).To(BeNil())
		})

		Context("when there is compatible data in postgres in a table with an 'id' column", func() {
			var currentTime time.Time

			BeforeEach(func() {
				currentTime = time.Now().UTC()

				stmt := fmt.Sprintf(`
				INSERT INTO table_with_id (
					id,
					name,
					ci_name,
					created_at,
					truthiness
				) VALUES (
					3,
					'some-name',
					'some-ci-name',
					'%s',
					true
				)`, currentTime.Format(time.RFC3339))
				result, err := pgRunner.DB().Exec(stmt)
				Expect(err).NotTo(HaveOccurred())
				rowsAffected, err := result.RowsAffected()
				Expect(err).NotTo(HaveOccurred())
				Expect(rowsAffected).To(BeNumerically("==", 1))
			})

			It("inserts the data into the target", func() {
				result, err := migrator.Migrate()
				Expect(err).NotTo(HaveOccurred())

				Expect(result).To(ContainElement(pg2mysql.MigrationResult{
					TableName:    "table_with_id",
					RowsMigrated: 1,
					RowsSkipped:  0,
				}))

				var id int
				var name string
				var ci_name string
				var created_at time.Time
				var truthiness bool

				stmt := "SELECT id, name, ci_name, created_at, truthiness FROM table_with_id WHERE id = 3"
				err = mysqlRunner.DB().QueryRow(stmt).Scan(&id, &name, &ci_name, &created_at, &truthiness)
				Expect(err).NotTo(HaveOccurred())

				Expect(id).To(Equal(3))
				Expect(name).To(Equal("some-name"))
				Expect(ci_name).To(Equal("some-ci-name"))
				Expect(created_at.Format(time.RFC1123Z)).To(Equal(currentTime.Format(time.RFC1123Z)))
				Expect(truthiness).To(BeTrue())
			})
		})

		Context("when there is compatible data in postgres in a table without an 'id' column", func() {
			var currentTime time.Time

			BeforeEach(func() {
				currentTime = time.Now().UTC()

				stmt := fmt.Sprintf(`
				INSERT INTO table_without_id (
					name,
					ci_name,
					created_at,
					truthiness
				) VALUES (
					'some-name',
					'some-ci-name',
					'%s',
					true
				)`, currentTime.Format(time.RFC3339))
				result, err := pgRunner.DB().Exec(stmt)
				Expect(err).NotTo(HaveOccurred())
				rowsAffected, err := result.RowsAffected()
				Expect(err).NotTo(HaveOccurred())
				Expect(rowsAffected).To(BeNumerically("==", 1))
			})

			It("inserts the data into the target", func() {
				result, err := migrator.Migrate()
				Expect(err).NotTo(HaveOccurred())

				Expect(result).To(ContainElement(pg2mysql.MigrationResult{
					TableName:    "table_without_id",
					RowsMigrated: 1,
					RowsSkipped:  0,
				}))

				var name string
				var ci_name string
				var created_at time.Time
				var truthiness bool

				stmt := "SELECT name, ci_name, created_at, truthiness FROM table_without_id WHERE name='some-name'"
				err = mysqlRunner.DB().QueryRow(stmt).Scan(&name, &ci_name, &created_at, &truthiness)
				Expect(err).NotTo(HaveOccurred())

				Expect(name).To(Equal("some-name"))
				Expect(ci_name).To(Equal("some-ci-name"))
				Expect(created_at.Format(time.RFC1123Z)).To(Equal(currentTime.Format(time.RFC1123Z)))
				Expect(truthiness).To(BeTrue())
			})
		})
	})
})
