package crawler

import (
	"fmt"
	"time"

	"github.com/rs/zerolog/log"
	"github.com/spf13/viper"
	"gorm.io/driver/postgres"
	"gorm.io/gorm"
)

type FundCrawler interface {
	CrawlFund(forDate *time.Time, cb SaveFund) []*Fund
}

var db *gorm.DB

func Conn() *gorm.DB {
	if db != nil {
		return db
	}
	dsn := fmt.Sprintf("host=%s user=%s password=%s dbname=%s port=%s sslmode=%s", viper.GetString("db.host"), viper.GetString("db.user"), viper.GetString("db.password"), viper.GetString("db.dbname"), viper.GetString("db.port"), viper.GetString("db.sslmode"))
	var err error
	gormLogger := &ZeroLogger{log: log.Logger}
	db, err = gorm.Open(postgres.Open(dsn), &gorm.Config{Logger: gormLogger})
	if err != nil {
		panic(err)
	}

	return db
}

type SaveFund func([]*Fund)
