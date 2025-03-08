package crawler

import (
	"time"

	"github.com/gocolly/colly/v2/debug"
	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
	"gorm.io/driver/postgres"
	"gorm.io/gorm"
)

type FundCrawler interface {
	CrawlFund(forDate *time.Time, cb SaveFund) []*Fund
}

type LogDebugger struct {
}

// Init initializes the LogDebugger
func (l *LogDebugger) Init() error {
	return nil
}

// Event receives Collector events and prints them to STDERR
func (l *LogDebugger) Event(e *debug.Event) {

	var lg *zerolog.Event
	if e.Type == "error" {
		lg = log.Error()
	} else {
		lg = log.Info()
	}

	lg.Uint32("CollectorID", e.CollectorID).
		Uint32("RequestID", e.RequestID).
		Str("Type", e.Type).
		Str("URL", e.Values["url"]).
		Timestamp().
		Msg("Crawler Event")
}

var db *gorm.DB

func Conn() *gorm.DB {
	if db != nil {
		return db
	}
	dsn := "host=localhost user=postgres password= dbname=postgres port=5432 sslmode=disable"
	var err error
	gormLogger := &ZeroLogger{log: log.Logger}
	db, err = gorm.Open(postgres.Open(dsn), &gorm.Config{Logger: gormLogger})
	if err != nil {
		panic(err)
	}

	if err = db.AutoMigrate(&FundManager{}); err != nil {
		log.Panic().Err(err).Msg("Error migrating FundManager")
	}
	if err = db.AutoMigrate(&Fund{}); err != nil {
		log.Panic().Err(err).Msg("Error migrating FundManager")
	}
	if err = db.AutoMigrate(&FundReport{}); err != nil {
		log.Panic().Err(err).Msg("Error migrating FundManager")
	}
	if err = db.AutoMigrate(&CrawlerEvent{}); err != nil {
		log.Panic().Err(err).Msg("Error migrating FundManager")
	}
	if err = db.SetupJoinTable(&FundManager{}, "Funds", &FundXFundManagers{}); err != nil {
		log.Panic().Err(err).Msg("Error migrating FundManager")
	}

	return db
}

type SaveFund func([]*Fund)
