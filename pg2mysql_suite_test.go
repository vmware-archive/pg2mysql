package pg2mysql_test

import (
	"fmt"
	"io/ioutil"
	"path/filepath"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	"github.com/pivotal-cf/pg2mysql/mysqlrunner"
	"github.com/pivotal-cf/pg2mysql/postgresrunner"

	"testing"
)

func TestPg2mysql(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "Pg2mysql Suite")
}

var mysqlRunner mysqlrunner.Runner
var pgRunner postgresrunner.Runner

var _ = BeforeSuite(func() {
	mysqlRunner = mysqlrunner.Runner{
		DBName: fmt.Sprintf("testdb_%d", GinkgoParallelNode()),
	}
	err := mysqlRunner.Setup()
	Expect(err).NotTo(HaveOccurred())

	bs, err := ioutil.ReadFile(filepath.Join("testdata", "mysqldata.sql"))
	Expect(err).NotTo(HaveOccurred())
	result, err := mysqlRunner.DB().Exec(fmt.Sprintf("USE %s;", mysqlRunner.DBName))
	Expect(err).NotTo(HaveOccurred())
	result, err = mysqlRunner.DB().Exec(string(bs))
	Expect(err).NotTo(HaveOccurred())
	rowsAffected, err := result.RowsAffected()
	Expect(err).NotTo(HaveOccurred())
	Expect(rowsAffected).To(BeNumerically("==", 0))

	pgRunner = postgresrunner.Runner{
		DBName: fmt.Sprintf("testdb_%d", GinkgoParallelNode()),
	}
	err = pgRunner.Setup()
	Expect(err).NotTo(HaveOccurred())

	bs, err = ioutil.ReadFile(filepath.Join("testdata", "pgdata.sql"))
	Expect(err).NotTo(HaveOccurred())
	result, err = pgRunner.DB().Exec(string(bs))
	Expect(err).NotTo(HaveOccurred())
	rowsAffected, err = result.RowsAffected()
	Expect(err).NotTo(HaveOccurred())
	Expect(rowsAffected).To(BeNumerically("==", 0))
})

var _ = AfterSuite(func() {
	err := mysqlRunner.Teardown()
	Expect(err).NotTo(HaveOccurred())
	err = pgRunner.Teardown()
	Expect(err).NotTo(HaveOccurred())
})

var _ = AfterEach(func() {
	err := mysqlRunner.Truncate()
	Expect(err).NotTo(HaveOccurred())
	err = pgRunner.Truncate()
	Expect(err).NotTo(HaveOccurred())
})
