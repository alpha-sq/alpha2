package main

import (
	"alpha2/crawler"
	"alpha2/crawler/pmf"
	"encoding/json"
	"os"
	"time"

	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
	"gorm.io/gorm/clause"
)

var Years []int = []int{2018, 2019, 2020, 2021, 2022, 2023, 2024, 2025}

func main() {
	forDate, _ := time.Parse("2006-01-02", "2024-01-01")
	craw := pmf.PMFCrawler{}
	db := crawler.Conn()
	craw.CrawlFund(&forDate, func(funds []*crawler.Fund) {
		for _, fund := range funds {
			err := db.Model(&crawler.Fund{}).Clauses(clause.OnConflict{
				DoNothing: true,
			}).Create(fund)
			if err.Error != nil {
				jsonfund, _ := json.Marshal(fund)
				log.Error().Err(err.Error).RawJSON("fund", jsonfund).Msg("Error while saving funds")
			}
		}
	})
}

func init() {
	zerolog.TimeFieldFormat = zerolog.TimeFormatUnix
	log.Logger = log.Output(zerolog.ConsoleWriter{Out: os.Stderr})
	log.Logger = log.Level(zerolog.ErrorLevel)
}
