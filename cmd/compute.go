/*
Copyright © 2025 NAME HERE <EMAIL ADDRESS>
*/
package cmd

import (
	"alpha2/crawler"
	"math"

	"github.com/rs/zerolog/log"
	"github.com/spf13/cobra"
	"gorm.io/gorm"
)

// computeCmd represents the compute command
var computeCmd = &cobra.Command{
	Use:   "compute",
	Short: "A brief description of your command",
	Long: `A longer description that spans multiple lines and likely contains examples
and usage of using your command. For example:

Cobra is a CLI library for Go that empowers applications.
This application is a tool to generate the needed files
to quickly create a Cobra application.`,
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
				Limit(5 * 12).
				Find(&reports).Error
			if err != nil {
				return err
			}

			if len(reports) != 0 {
				fund.OverAllReturns = reports[0].OverAllReturns
				fund.Month1Returns = reports[0].Month1Returns
			}

			if len(reports) >= 3 {

				// Calculate 3-month return
				var returns []float64
				for i := range 3 {
					returns = append(returns, *reports[i].Month1Returns)
				}
				threeMonthReturn := calculateReturnsWithMonthData(returns)
				fund.Month3Returns = &threeMonthReturn
				// Print the 3-month return
				log.Info().Str("fund", fund.Name).Float64("3-month-return", threeMonthReturn).Msg("")
			}

			if len(reports) >= 6 {

				// Calculate 6-month return
				var returns []float64
				for i := range 6 {
					returns = append(returns, *reports[i].Month1Returns)
				}
				sixMonthReturn := calculateReturnsWithMonthData(returns)
				fund.Month6Returns = &sixMonthReturn
				// Print the 6-month return
				log.Info().Str("fund", fund.Name).Float64("6-month-return", sixMonthReturn).Msg("")
			}
			if len(reports) >= 12 {
				// Calculate 12-month return
				var returns []float64
				for i := range 12 {
					returns = append(returns, *reports[i].Month1Returns)
				}
				twelveMonthReturn := calculateReturnsWithMonthData(returns)
				fund.Yr1Returns = &twelveMonthReturn
				// Print the 12-month return
				log.Info().Str("fund", fund.Name).Float64("12-month-return", twelveMonthReturn).Msg("")
			}

			if len(reports) >= 12*2 {

				var returns []float64
				for i := range 12 * 2 {
					returns = append(returns, *reports[i].Month1Returns)
				}
				twoYearReturn := calculateReturnsWithMonthData(returns)
				fund.Yr2Returns = &twoYearReturn
				yr2Cagr := returnsToCagr(twoYearReturn, 2)
				fund.Yr2Cagr = &yr2Cagr
				// Print the 2-year return
				log.Info().Str("fund", fund.Name).Float64("2-year-return", twoYearReturn).Msg("")
			}

			if len(reports) >= 12*3 {

				var returns []float64
				for i := range 12 * 3 {
					returns = append(returns, *reports[i].Month1Returns)
				}
				threeYearReturn := calculateReturnsWithMonthData(returns)
				fund.Yr3Returns = &threeYearReturn
				Yr3Cagr := returnsToCagr(threeYearReturn, 3)
				fund.Yr3Cagr = &Yr3Cagr

				// Print the 3-year return
				log.Info().Str("fund", fund.Name).Float64("3-year-return", threeYearReturn).Msg("")
			}

			if len(reports) >= 12*4 {

				var returns []float64
				for i := range 12 * 4 {
					returns = append(returns, *reports[i].Month1Returns)
				}
				fourYearReturn := calculateReturnsWithMonthData(returns)
				fund.Yr4Returns = &fourYearReturn
				Yr4Cagr := returnsToCagr(fourYearReturn, 4)
				fund.Yr4Cagr = &Yr4Cagr
				// Print the 4-year return
				log.Info().Str("fund", fund.Name).Float64("4-year-return", fourYearReturn).Msg("")
			}

			if len(reports) >= 12*5 {
				var returns []float64
				for i := range 12 * 5 {
					returns = append(returns, *reports[i].Month1Returns)
				}
				fiveYearReturn := calculateReturnsWithMonthData(returns)
				fund.Yr5Returns = &fiveYearReturn
				Yr5Cagr := returnsToCagr(fiveYearReturn, 5)
				fund.Yr5Cagr = &Yr5Cagr
				// Print the 5-year return
				log.Info().Str("fund", fund.Name).Float64("5-year-return", fiveYearReturn).Msg("")
			}

			if err := db.Save(&fund).Error; err != nil {
				log.Error().Err(err).Msg("Failed to save fund returns")
				return err
			}

			return nil
		})
	},
}

func calculateReturnsWithMonthData(returns []float64) float64 {
	totalReturn := 1.0
	for _, r := range returns {
		totalReturn *= (1 + r/100) // Convert percentage to decimal
	}
	return (totalReturn - 1) * 100 // Return as percentage
}

func returnsToCagr(r float64, yr int) float64 {
	return (math.Pow(r/100+1, float64(1/yr)) - 1) * 100
}

func init() {
	rootCmd.AddCommand(computeCmd)

	// Here you will define your flags and configuration settings.

	// Cobra supports Persistent Flags which will work for this command
	// and all subcommands, e.g.:
	// computeCmd.PersistentFlags().String("foo", "", "A help for foo")

	// Cobra supports local flags which will only run when this command
	// is called directly, e.g.:
	// computeCmd.Flags().BoolP("toggle", "t", false, "Help message for toggle")
}
