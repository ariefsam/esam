package sqlite

import (
	"encoding/json"
	"errors"
	"log"
	"os"
	"time"

	"github.com/ariefsam/esam"
	"github.com/ariefsam/esam/idgenerator"

	sq "gorm.io/driver/sqlite"
	"gorm.io/gorm"
	"gorm.io/gorm/logger"
)

type EventStoreSqlite struct {
	db *gorm.DB
}

type GormEvent struct {
	gorm.Model
	EventID string `gorm:"column:event_id;index:event_id_idx" json:"event_id"`
	esam.EventMetadata
}

func (es *EventStoreSqlite) Store(eventName string, data interface{}, userId string, timestamp int64) (id string, err error) {
	if timestamp == 0 {
		timestamp = time.Now().Unix()
	}
	dataJs, err := json.Marshal(data)
	if err != nil {
		return
	}
	dataEvent := GormEvent{}
	dataEvent.Name = eventName
	dataEvent.UserId = userId
	dataEvent.Data = string(dataJs)
	dataEvent.Timestamp = timestamp
	dataEvent.EventID = idgenerator.Generate()
	id = dataEvent.EventID
	err = es.db.Create(&dataEvent).Error

	return
}

func (es *EventStoreSqlite) Next(cursorID string) (eventMetadata *esam.EventMetadata, err error) {
	var temp, gormEvent GormEvent
	if cursorID == "" {
		err = es.db.Take(&gormEvent).Error
		if err != nil {
			if !errors.Is(err, gorm.ErrRecordNotFound) {
				log.Println(err)
				return
			}
			err = nil
			return
		}

		goto decode

	}

	err = es.db.Where("event_id=?", cursorID).Take(&temp).Error
	if err != nil {
		if !errors.Is(err, gorm.ErrRecordNotFound) {
			log.Println(err)
			return
		}
		err = nil
		return
	}

	err = es.db.Where("id>?", temp.ID).Take(&gormEvent).Error
	if err != nil {
		if !errors.Is(err, gorm.ErrRecordNotFound) {
			log.Println(err)
			return
		}
		err = nil
		return
	}

decode:
	eventMetadata = &gormEvent.EventMetadata
	eventMetadata.EventID = gormEvent.EventID

	return
}

func NewEventStore(filepath string) (es EventStoreSqlite, err error) {
	logService := logger.New(log.New(os.Stdout, "\r\n", log.LstdFlags), // io writer
		logger.Config{
			SlowThreshold:             time.Second,   // Slow SQL threshold
			LogLevel:                  logger.Silent, // Log level
			IgnoreRecordNotFoundError: true,          // Ignore ErrRecordNotFound error for logger
			Colorful:                  false,         // Disable color
		})
	db, err := gorm.Open(sq.Open(filepath), &gorm.Config{
		Logger: logService,
	})
	if err != nil {
		log.Println(err)
		return
	}

	d, err := db.DB()
	if err != nil {
		log.Println(err)
		return
	}
	d.SetMaxOpenConns(1)
	es.db = db
	err = db.AutoMigrate(&GormEvent{})
	return
}
