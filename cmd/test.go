/*
Copyright © 2025 NAME HERE <EMAIL ADDRESS>
*/
package cmd

import (
	"alpha2/crawler/mf"
	"alpha2/jobs"
	"context"
	"time"

	"github.com/reugn/go-quartz/quartz"
	"github.com/spf13/cobra"
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
		job := &mf.MFSync{}
		jd := quartz.NewJobDetail(job, quartz.NewJobKeyWithGroup("init", "MFSync"))
		t := quartz.NewRunOnceTrigger(time.Second * 5)

		jobs.Scheduler.ScheduleJob(jd, t)

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
