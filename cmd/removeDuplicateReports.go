/*
Copyright © 2025 NAME HERE <EMAIL ADDRESS>
*/
package cmd

import (
	"alpha2/crawler"
	"strconv"

	"github.com/rs/zerolog/log"
	"github.com/samber/lo"
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

		var funds []crawler.Fund
		err := db.Where("type = 'PMF' and other_data != 'null'").FindInBatches(&funds, 1, func(tx *gorm.DB, batch int) error {
			for _, fund := range funds {

				var reports []*crawler.FundReport
				tx.Where("fund_id = ?", fund.ID).Find(&reports)
				if len(reports) == 0 {
					log.Info().Msgf("No reports found for fund %d on date %s", fund.ID, fund.OtherData)
					continue
				}

				if fund.OtherData == nil {
					log.Warn().Msgf("No other_data found for fund %d on date %s", fund.ID, fund.OtherData)
					continue
				}

				idStr := fund.OtherData["original_id"]
				if idStr == "" {
					log.Warn().Msgf("No original_id found for fund %d on date %s", fund.ID, fund.OtherData)
					continue
				}
				originalID, err := strconv.ParseUint(idStr, 10, 64)
				if err != nil {
					log.Error().Err(err).Msgf("Failed to parse original_id for fund %d on date %s", fund.ID, fund.OtherData)
					return err
				}

				if originalID == fund.ID {
					log.Warn().Msgf("Original ID is the same as fund ID for fund %d on date %s", fund.ID, fund.OtherData)
					continue
				}

				lo.ForEach(reports, func(report *crawler.FundReport, idx int) {
					report.ID = 0
					report.FundID = originalID

					if report.OtherData == nil {
						report.OtherData = crawler.JSONB{}
					}

					report.OtherData["priority"] = "low"
				})

				err = tx.Create(&reports).Error
				if err != nil {
					log.Error().Err(err).Msgf("Failed to create reports for fund %d on date %s", fund.ID, fund.OtherData)
					return err
				}

				// if tx.RowsAffected > 0 {
				// 	log.Info().Msgf("Deleted %d duplicate reports for fund %d on date %s", tx.RowsAffected, report.FundID, report.ReportDate)
				// }
			}

			return nil
		}).Error

		if err != nil {
			log.Error().Err(err).Msg("Failed to remove duplicate reports")
		} else {
			log.Info().Msg("Successfully removed duplicate reports")
		}
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
