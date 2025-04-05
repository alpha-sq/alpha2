/*
Copyright © 2025 NAME HERE <EMAIL ADDRESS>
*/
package cmd

import (
	"alpha2/crawler"
	"time"

	"github.com/rs/zerolog/log"
	"github.com/spf13/cobra"
	"gorm.io/gorm"
)

// removeDuplicateReportsCmd represents the removeDuplicateReports command
var removeDuplicateReportsCmd = &cobra.Command{
	Use:   "removeDuplicateReports",
	Short: "A brief description of your command",
	Long: `A longer description that spans multiple lines and likely contains examples
and usage of using your command. For example:

Cobra is a CLI library for Go that empowers applications.
This application is a tool to generate the needed files
to quickly create a Cobra application.`,
	Run: func(cmd *cobra.Command, args []string) {
		db := crawler.Conn()

		var reports []crawler.FundReport
		now := time.Now()
		now = now.AddDate(0, -6, 0)
		db.Where("report_date > ?", now).Order("other_data->>'merged_id'").FindInBatches(&reports, 1, func(tx *gorm.DB, batch int) error {
			for _, report := range reports {
				if report.ID == 0 {
					continue
				}

				tx := db.
					Where("fund_id = ? and report_date = ? and id != ?", report.FundID, report.ReportDate, report.ID).
					Delete(&crawler.FundReport{})

				if tx.Error != nil {
					return tx.Error
				}

				if tx.RowsAffected > 0 {
					log.Info().Msgf("Deleted %d duplicate reports for fund %d on date %s", tx.RowsAffected, report.FundID, report.ReportDate)
				}
			}

			return nil
		})
	},
}

func init() {
	rootCmd.AddCommand(removeDuplicateReportsCmd)

	// Here you will define your flags and configuration settings.

	// Cobra supports Persistent Flags which will work for this command
	// and all subcommands, e.g.:
	// removeDuplicateReportsCmd.PersistentFlags().String("foo", "", "A help for foo")

	// Cobra supports local flags which will only run when this command
	// is called directly, e.g.:
	// removeDuplicateReportsCmd.Flags().BoolP("toggle", "t", false, "Help message for toggle")
}
