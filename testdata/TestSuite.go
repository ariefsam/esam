package testdata

import (
	"context"
	"testing"
	"time"

	"github.com/ariefsam/esam"

	"github.com/stretchr/testify/assert"
)

func TestSuite(t *testing.T, es esam.EventStore, proj esam.Projection) {
	userID := "user001"
	type Data struct {
		ID   string
		Name string
	}

	var data Data
	data.ID = "id001"
	data.Name = "Helo"
	timestamp := int64(1400)
	id1, err := es.Store("ev01", &data, userID, timestamp)
	assert.NoError(t, err)
	assert.NotEmpty(t, id1)

	meta, err := es.Next("")
	assert.NoError(t, err)
	assert.NotEmpty(t, data, meta.Data)
	assert.Equal(t, timestamp, meta.Timestamp)

	data.ID = "id002"
	data.Name = "Helo2"
	timestamp = int64(1401)
	_, err = es.Store("ev01", &data, userID, timestamp)
	assert.NoError(t, err)

	meta, err = es.Next(id1)
	assert.NoError(t, err)
	assert.NotEmpty(t, data, meta.Data)
	assert.Equal(t, timestamp, meta.Timestamp)

	var obj ObjProjection
	proj.RegisterEvent("ev01", &obj)
	ctx, _ := context.WithTimeout(context.TODO(), 2*time.Second)
	proj.Project(ctx)
	assert.Equal(t, data.ID, idTest)
}

var idTest string

type ObjProjection struct {
	ID   string
	Data string
}

func (o *ObjProjection) Process(data esam.EventMetadata) (err error) {
	idTest = o.ID
	return
}
