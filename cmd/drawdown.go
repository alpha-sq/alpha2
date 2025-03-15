/*
Copyright © 2025 NAME HERE <EMAIL ADDRESS>
*/
package cmd

import (
	"alpha2/crawler"
	"math"
	"time"

	"github.com/rs/zerolog/log"
	"github.com/spf13/cobra"
	"gorm.io/gorm"
)

var noOfYears int

// drawdownCmd represents the drawdown command
var drawdownCmd = &cobra.Command{
	Use:   "drawdown",
	Short: "Compute the maximum drawdown of a fund",
	Long:  ` Compute the maximum drawdown of a fund. The maximum drawdown is the maximum loss from a peak to a trough of a portfolio, before a new peak is attained.`,
	Run: func(cmd *cobra.Command, args []string) {
		db := crawler.Conn()

		var funds []crawler.Fund
		db.FindInBatches(&funds, 1, func(tx *gorm.DB, batch int) error {
			fund := funds[0]

			var reports []crawler.FundReport
			err := db.Where(&crawler.FundReport{
				FundID: fund.ID,
			}).
				Where("month1_returns IS NOT NULL").
				Order("report_date desc").
				Limit(noOfYears * 12).
				Find(&reports).Error
			if err != nil {
				log.Error().Err(err).Msg("Failed to fetch report data")
				return err
			}

			if len(reports) < noOfYears*12 {
				log.Warn().Msg("Insufficient data for drawdown calculation")
				return nil
			}

			// Calculate Cumulative Return
			fundValue := 1.0 // Starting value (Base 1 for easy growth calculation)
			results := []struct {
				ReportDate *time.Time
				FundValue  float64
			}{}

			for _, report := range reports {
				fundValue *= 1 + (*report.Month1Returns / 100)
				results = append(results, struct {
					ReportDate *time.Time
					FundValue  float64
				}{report.ReportDate, fundValue})
			}

			// Display results
			for _, result := range results {
				log.Info().Str("date", result.ReportDate.String()).Float64("fund_value", result.FundValue).Msg("Fund Value")
			}

			// Max Drawdown Calculation
			maxDrawdown := computeMaxDrawdown(results)
			if noOfYears == 3 {
				fund.MaxDrawdown3Yrs = &maxDrawdown
			} else {
				fund.MaxDrawdown5Yr = &maxDrawdown
			}

			if err := db.Save(&fund).Error; err != nil {
				log.Error().Err(err).Msg("Failed to save max drawdown")
				return err
			}
			return nil
		})

	},
}

func computeMaxDrawdown(data []struct {
	ReportDate *time.Time
	FundValue  float64
}) float64 {
	if len(data) == 0 {
		return 0
	}

	maxPeak := data[0].FundValue
	maxDrawdown := 0.0

	for _, entry := range data {
		if entry.FundValue > maxPeak {
			maxPeak = entry.FundValue
		}
		drawdown := (maxPeak - entry.FundValue) / maxPeak
		maxDrawdown = math.Max(maxDrawdown, drawdown)
	}

	return maxDrawdown
}

func init() {
	rootCmd.AddCommand(drawdownCmd)

	// Here you will define your flags and configuration settings.

	// Cobra supports Persistent Flags which will work for this command
	// and all subcommands, e.g.:
	// drawdownCmd.PersistentFlags().String("foo", "", "A help for foo")

	// Cobra supports local flags which will only run when this command
	// is called directly, e.g.:
	// drawdownCmd.Flags().BoolP("toggle", "t", false, "Help message for toggle")
	drawdownCmd.Flags().IntVar(&noOfYears, "years", 3, "Number of months to consider for drawdown calculation")
}
