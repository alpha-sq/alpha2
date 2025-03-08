/*
Copyright © 2025 NAME HERE <EMAIL ADDRESS>
*/
package cmd

import (
	"alpha2/crawler"
	"alpha2/crawler/pmf"
	"encoding/json"

	"github.com/rs/zerolog/log"
	"github.com/spf13/cobra"
	"gorm.io/gorm/clause"
)

// retryCmd represents the retry command
var retryCmd = &cobra.Command{
	Use:   "retry",
	Short: "Retry failed crawl",
	Long:  `Retry failed crawl. This command will retry the failed crawl from the last successful crawl.`,
	Run: func(cmd *cobra.Command, args []string) {
		crawl := pmf.NewPMFCrawler()
		db := crawler.Conn()
		crawl.ReTryFailed(func(funds []*crawler.Fund) {
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
