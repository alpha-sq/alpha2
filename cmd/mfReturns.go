/*
Copyright © 2025 NAME HERE <EMAIL ADDRESS>
*/
package cmd

import (
	"alpha2/crawler"
	"alpha2/crawler/mf"
	"alpha2/jobs"
	"context"
	"strconv"
	"time"

	"github.com/reugn/go-quartz/quartz"
	"github.com/spf13/cobra"
	"gorm.io/gorm"
)

// mfReturnsCmd represents the mfReturns command
var mfReturnsCmd = &cobra.Command{
	Use:   "mfReturns",
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

		var funds []mf.MutualFundData
		db.FindInBatches(&funds, 100, func(tx *gorm.DB, batch int) error {
			db := crawler.Conn()
			for _, fund := range funds {
				cFund := &crawler.Fund{
					Name: fund.Name,
					Type: "MF",
					OtherData: crawler.JSONB{
						"mf_fund_id": strconv.FormatUint(uint64(fund.ID), 10),
					},
				}
				if err := db.Save(cFund).Error; err != nil {
					return err
				}
				job := &mf.MFRetuns{
					FundID: cFund.ID,
				}
				jd := quartz.NewJobDetail(job, quartz.NewJobKeyWithGroup("init", "MFRetuns"))
				t := quartz.NewRunOnceTrigger(time.Second * 5)
				jobs.Scheduler.ScheduleJob(jd, t)
			}
			return nil
		})

		jobs.Scheduler.Wait(ctx)
	},
}

func init() {
	rootCmd.AddCommand(mfReturnsCmd)

	// Here you will define your flags and configuration settings.

	// Cobra supports Persistent Flags which will work for this command
	// and all subcommands, e.g.:
	// mfReturnsCmd.PersistentFlags().String("foo", "", "A help for foo")

	// Cobra supports local flags which will only run when this command
	// is called directly, e.g.:
	// mfReturnsCmd.Flags().BoolP("toggle", "t", false, "Help message for toggle")
}
