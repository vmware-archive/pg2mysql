package pg2mysql_test

import (
	"fmt"
	"time"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	"github.com/pivotal-cf/pg2mysql"
	"github.com/pivotal-cf/pg2mysql/pg2mysqlfakes"
)

var _ = Describe("Migrator", func() {
	var (
		migrator      pg2mysql.Migrator
		mysql         pg2mysql.DB
		pg            pg2mysql.DB
		truncateFirst bool
		watcher       *pg2mysqlfakes.FakeMigratorWatcher
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

		watcher = &pg2mysqlfakes.FakeMigratorWatcher{}
		migrator = pg2mysql.NewMigrator(pg, mysql, truncateFirst, watcher)
	})

	AfterEach(func() {
		err := mysql.Close()
		Expect(err).NotTo(HaveOccurred())
		err = pg.Close()
		Expect(err).NotTo(HaveOccurred())
	})

	Describe("Migrate", func() {
		It("notifies the watcher", func() {
			err := migrator.Migrate()
			Expect(err).NotTo(HaveOccurred())
			Expect(watcher.TableMigrationDidStartCallCount()).To(Equal(2))
			Expect(watcher.TableMigrationDidFinishCallCount()).To(Equal(2))

			expected := map[string]int64{
				"table_with_id":    0,
				"table_without_id": 0,
			}

			tableName, missingRows := watcher.TableMigrationDidFinishArgsForCall(0)
			Expect(missingRows).To(Equal(expected[tableName]))
			tableName, missingRows = watcher.TableMigrationDidFinishArgsForCall(1)
			Expect(missingRows).To(Equal(expected[tableName]))
		})

		It("does not insert any data into the target", func() {
			err := migrator.Migrate()
			Expect(err).NotTo(HaveOccurred())

			var count int64
			err = mysqlRunner.DB().QueryRow("SELECT COUNT(1) from table_with_id").Scan(&count)
			Expect(err).NotTo(HaveOccurred())
			Expect(count).To(BeZero())

			err = mysqlRunner.DB().QueryRow("SELECT COUNT(1) from table_with_id").Scan(&count)
			Expect(err).NotTo(HaveOccurred())
			Expect(count).To(BeZero())
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
					$$some'name$$,
					$$some'ci'name$$,
					'%s',
					true
				)`, currentTime.Format(time.RFC3339))
				result, err := pgRunner.DB().Exec(stmt)
				Expect(err).NotTo(HaveOccurred())
				rowsAffected, err := result.RowsAffected()
				Expect(err).NotTo(HaveOccurred())
				Expect(rowsAffected).To(BeNumerically("==", 1))
			})

			It("notifies the watcher", func() {
				err := migrator.Migrate()
				Expect(err).NotTo(HaveOccurred())
				Expect(watcher.TableMigrationDidStartCallCount()).To(Equal(2))
				Expect(watcher.TableMigrationDidFinishCallCount()).To(Equal(2))

				expected := map[string]int64{
					"table_with_id":    1,
					"table_without_id": 0,
				}

				tableName, missingRows := watcher.TableMigrationDidFinishArgsForCall(0)
				Expect(missingRows).To(Equal(expected[tableName]))
				tableName, missingRows = watcher.TableMigrationDidFinishArgsForCall(1)
				Expect(missingRows).To(Equal(expected[tableName]))
			})

			It("inserts the data into the target", func() {
				err := migrator.Migrate()
				Expect(err).NotTo(HaveOccurred())

				var id int
				var name string
				var ci_name string
				var created_at time.Time
				var truthiness bool

				stmt := "SELECT id, name, ci_name, created_at, truthiness FROM table_with_id WHERE id = 3"
				err = mysqlRunner.DB().QueryRow(stmt).Scan(&id, &name, &ci_name, &created_at, &truthiness)
				Expect(err).NotTo(HaveOccurred())

				Expect(id).To(Equal(3))
				Expect(name).To(Equal("some'name"))
				Expect(ci_name).To(Equal("some'ci'name"))
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
					$$some'name$$,
					$$some'ci'name$$,
					'%s',
					true
				)`, currentTime.Format(time.RFC3339))
				result, err := pgRunner.DB().Exec(stmt)
				Expect(err).NotTo(HaveOccurred())
				rowsAffected, err := result.RowsAffected()
				Expect(err).NotTo(HaveOccurred())
				Expect(rowsAffected).To(BeNumerically("==", 1))
			})

			It("notifies the watcher", func() {
				err := migrator.Migrate()
				Expect(err).NotTo(HaveOccurred())
				Expect(watcher.TableMigrationDidStartCallCount()).To(Equal(2))
				Expect(watcher.TableMigrationDidFinishCallCount()).To(Equal(2))

				expected := map[string]int64{
					"table_with_id":    0,
					"table_without_id": 1,
				}

				tableName, missingRows := watcher.TableMigrationDidFinishArgsForCall(0)
				Expect(missingRows).To(Equal(expected[tableName]))
				tableName, missingRows = watcher.TableMigrationDidFinishArgsForCall(1)
				Expect(missingRows).To(Equal(expected[tableName]))
			})

			It("inserts the data into the target", func() {
				err := migrator.Migrate()
				Expect(err).NotTo(HaveOccurred())

				var name string
				var ci_name string
				var created_at time.Time
				var truthiness bool

				stmt := `SELECT name, ci_name, created_at, truthiness FROM table_without_id WHERE name="some'name"`
				err = mysqlRunner.DB().QueryRow(stmt).Scan(&name, &ci_name, &created_at, &truthiness)
				Expect(err).NotTo(HaveOccurred())

				Expect(name).To(Equal("some'name"))
				Expect(ci_name).To(Equal("some'ci'name"))
				Expect(created_at.Format(time.RFC1123Z)).To(Equal(currentTime.Format(time.RFC1123Z)))
				Expect(truthiness).To(BeTrue())
			})
		})
	})
})
