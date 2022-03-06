package esam

import (
	"context"
)

type EventStore interface {
	Store(eventName string, data interface{}, userId string, timestamp int64) (id string, err error)
	Next(id string) (em *EventMetadata, err error)
}

type Projection interface {
	RegisterEvent(eventName string, target EventProjection) (err error)
	Project(ctx context.Context)
}

type EventProjection interface {
	Process(em EventMetadata) (err error)
}
