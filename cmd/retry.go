/*
Copyright © 2025 NAME HERE <EMAIL ADDRESS>
*/
package cmd

import (
	"alpha2/crawler"
	"alpha2/crawler/pmf"
	"alpha2/jobs"
	"time"

	"github.com/reugn/go-quartz/quartz"
	"github.com/rs/zerolog/log"
	"github.com/samber/lo"
	"github.com/spf13/cobra"
)

// retryCmd represents the retry command
var retryCmd = &cobra.Command{
	Use:   "retry",
	Short: "Retry failed crawl",
	Long:  `Retry failed crawl. This command will retry the failed crawl from the last successful crawl.`,
	Run: func(cmd *cobra.Command, args []string) {
		db := crawler.Conn()
		jobs.Init()

		var managers []crawler.FundManager
		err := db.Model(&crawler.FundManager{}).Find(&managers).Error
		if err != nil {
			log.Error().Err(err).Msg("Failed to fetch fund managers")
			return
		}
		if len(managers) == 0 {
			log.Info().Msg("No fund managers found")
			return
		}
		for _, manager := range managers {
			startDate := time.Date(2025, 5, 1, 0, 0, 0, 0, time.UTC)
			endDate := time.Date(2025, 7, 1, 0, 0, 0, 0, time.UTC)
			UID := manager.OtherData["UID"]
			for startDate.Before(endDate) {
				forDate := time.Date(startDate.Year(), startDate.Month(), 1, 0, 0, 0, 0, time.UTC)

				job := &pmf.CrawlPMFFunds{
					UID:      UID,
					ForDate:  forDate.Format(time.DateOnly),
					SkipNext: true,
				}
				randJobID := lo.RandomString(10, []rune("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789"))
				jd := quartz.NewJobDetail(job, quartz.NewJobKeyWithGroup(randJobID, "CrawlPMFFunds"))
				t := quartz.NewRunOnceTrigger(time.Second * 5)
				err := jobs.Scheduler.ScheduleJob(jd, t)
				if err != nil {
					log.Error().Err(err).Msg("Failed to schedule job")
					continue
				}

				startDate = time.Date(startDate.Year(), startDate.Month()+1, 1, 0, 0, 0, 0, time.UTC)
			}
		}

	},
}

func init() {
	rootCmd.AddCommand(retryCmd)

	// Here you will define your flags and configuration settings.

	// Cobra supports Persistent Flags which will work for this command
	// and all subcommands, e.g.:
	// retryCmd.PersistentFlags().String("foo", "", "A help for foo")

	// Cobra supports local flags which will only run when this command
	// is called directly, e.g.:
	// retryCmd.Flags().BoolP("toggle", "t", false, "Help message for toggle")
}
