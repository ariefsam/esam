package sqlite_test

import (
	"log"
	"os"
	"testing"

	sqlite "github.com/ariefsam/esam/driver/sqlite"

	"github.com/ariefsam/esam/testdata"

	"github.com/stretchr/testify/assert"
)

func TestNewSQLite(t *testing.T) {
	log.SetFlags(log.LstdFlags | log.Lshortfile)
	os.Remove("./testdata.db")
	os.Remove("./testdataprojection.db")
	var err error

	var es sqlite.EventStoreSqlite
	es, err = sqlite.NewEventStore("testdata.db")
	assert.NoError(t, err)
	assert.NotNil(t, es)

	var projection sqlite.ProjectionSqlite
	projection, err = sqlite.NewProjection("testdataprojection.db", &es)
	assert.NoError(t, err)
	assert.NotEmpty(t, &projection)

	testdata.TestSuite(t, &es, &projection)
	// os.Remove("./testdata.db")
	// os.Remove("./testdataprojection.db")
}
