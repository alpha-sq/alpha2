/*
Copyright © 2025 NAME HERE <EMAIL ADDRESS>
*/
package cmd

import (
	"alpha2/crawler"
	"alpha2/crawler/pmf"
	"alpha2/jobs"
	"context"
	"time"

	"github.com/reugn/go-quartz/quartz"
	"github.com/rs/zerolog/log"
	"github.com/spf13/cobra"
	"gorm.io/gorm"
)

// testCmd represents the test command
var testCmd = &cobra.Command{
	Use:   "test",
	Short: "A brief description of your command",
	Long: `A longer description that spans multiple lines and likely contains examples
and usage of using your command. For example:

Cobra is a CLI library for Go that empowers applications.
This application is a tool to generate the needed files
to quickly create a Cobra application.`,
	Run: func(cmd *cobra.Command, args []string) {
		jobs.Init()
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		jobs.Scheduler.Start(ctx)

		db := crawler.Conn()

		managers := []crawler.FundManager{}
		err := db.Where(&crawler.FundManager{
			ID: 6,
		}).FindInBatches(&managers, 1, func(tx *gorm.DB, batch int) error {
			UID := managers[0].OtherData["UID"]
			if UID == "" {
				return nil
			}

			job := &pmf.CrawlPMFFunds{
				UID:     UID,
				ForDate: "2025-02-01",
			}
			jd := quartz.NewJobDetail(job, quartz.NewJobKeyWithGroup("ifnit:2025-02-01:"+UID, "CrawlPMFFunds"))
			t := quartz.NewRunOnceTrigger(time.Second * 5)
			return jobs.Scheduler.ScheduleJob(jd, t)
		}).Error
		if err != nil {
			log.Error().Err(err).Msg("Error in FindInBatches")
		}

		jobs.Scheduler.Wait(ctx)

		// mfCrawler := mf.NewMutualFundCrawler()

		// db := crawler.Conn()
		// var res []*mf.MutualFundData
		// db.Where("id = ?", 163).FindInBatches(&res, 1, func(tx *gorm.DB, batch int) error {
		// 	navs, err := mfCrawler.CrawlFundNav(res[0])
		// 	if err != nil {
		// 		return err
		// 	}
		// 	if len(navs) == 0 {
		// 		return nil
		// 	}
		// 	db.Clauses(clause.OnConflict{
		// 		DoNothing: true,
		// 	}).Save(&navs)

		// 	return nil
		// })

		// if len(funds) == 0 {
		// 	return
		// }
		// db.Clauses(clause.OnConflict{
		// 	DoNothing: true,
		// }).Save(&funds)
	},
}

func init() {
	rootCmd.AddCommand(testCmd)
}
