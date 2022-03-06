package sqlite

import (
	"context"
	"encoding/json"
	"errors"
	"log"
	"os"
	"sync"
	"time"

	"github.com/ariefsam/esam"

	sq "gorm.io/driver/sqlite"
	"gorm.io/gorm"
	"gorm.io/gorm/logger"
)

type ProjectionSqlite struct {
	db     *gorm.DB
	es     esam.EventStore
	m      sync.Mutex
	events map[string][]esam.EventProjection
}

type GormProjection struct {
	gorm.Model
	EventID string
}

func (p *ProjectionSqlite) RegisterEvent(eventName string, prObj esam.EventProjection) (err error) {
	if p == nil {
		return
	}

	p.m.Lock()
	if p.events == nil {
		p.events = map[string][]esam.EventProjection{}
	}

	val, ok := p.events[eventName]
	if !ok {
		val = []esam.EventProjection{}
	}
	val = append(val, prObj)
	p.events[eventName] = val
	defer p.m.Unlock()
	return
}

func (p *ProjectionSqlite) Project(ctx context.Context) {
	once, _ := ctx.Value("once").(bool)

	if p.events == nil {
		p.events = map[string][]esam.EventProjection{}
	}
	var proj GormProjection
	err := p.db.Order("id desc").Limit(1).Take(&proj).Error
	if err != nil {
		if !errors.Is(err, gorm.ErrRecordNotFound) {
			log.Println(err)
			return
		}
	}
	var id string
	id = proj.EventID
	for {
		select {
		case <-ctx.Done():
			return
		default:
			meta, err := p.es.Next(id)
			if err != nil {
				log.Println(err)
				if once {
					return
				}
				continue
			}
			if meta == nil {
				if once {
					return
				}
				time.Sleep(1 * time.Second)
				continue
			}
			val, ok := p.events[meta.Name]
			if ok {
				for _, v := range val {
					err = json.Unmarshal([]byte(meta.Data), v)
					if err != nil {
						log.Println(err)
						continue
					}
					v.Process(*meta)
				}
			} else {
				log.Println("projection not interested in event:", meta.Name)
			}
			var store GormProjection
			store.EventID = meta.EventID
			p.db.Create(&store)
			id = meta.EventID
		}
		if once {
			return
		}
	}
}

func NewProjection(filepath string, es esam.EventStore) (p ProjectionSqlite, err error) {
	logService := logger.New(log.New(os.Stdout, "\r\n", log.LstdFlags), // io writer
		logger.Config{
			SlowThreshold:             time.Second,   // Slow SQL threshold
			LogLevel:                  logger.Silent, // Log level
			IgnoreRecordNotFoundError: true,          // Ignore ErrRecordNotFound error for logger
			Colorful:                  false,         // Disable color
		})
	p.es = es
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
	p.db = db
	db.AutoMigrate(&GormProjection{})
	return
}
