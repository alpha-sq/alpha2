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

// sharpeRatioCmd represents the sharpeRatio command
var sharpeRatioCmd = &cobra.Command{
	Use:   "sharpeRatio",
	Short: " A Sharpe ratio is a measure of risk-adjusted return of an investment asset or a trading strategy.",
	Long:  ` A Sharpe ratio is a measure of risk-adjusted return of an investment asset or a trading strategy.`,
	Run: func(cmd *cobra.Command, args []string) {
		db := crawler.Conn()
		var funds []crawler.Fund
		db.FindInBatches(&funds, 1, func(tx *gorm.DB, batch int) error {
			fund := funds[0]

			var reports []crawler.FundReport
			threeYearsAgo := time.Now().AddDate(-noOfYears, 0, 0)
			err := db.Where("fund_id = ? AND month1_returns IS NOT NULL AND report_date >= ?", fund.ID, threeYearsAgo).
				Order("report_date desc").
				Limit(noOfYears * 12).
				Find(&reports).Error
			if err != nil {
				log.Error().Err(err).Uint64("fund_id", fund.ID).Msg("Failed to fetch report data")
				return err
			}

			if len(reports) < noOfYears*12 {
				log.Warn().Uint64("fund_id", fund.ID).Msg("Insufficient data for sharpe ratio calculation")
				return nil
			}

			// Calculate mean return
			totalReturn := 0.0
			returns := []float64{}

			for _, report := range reports {
				totalReturn += *report.Month1Returns
				returns = append(returns, *report.Month1Returns)
			}

			meanReturn := totalReturn / float64(len(returns))

			// Calculate standard deviation (volatility)
			var varianceSum float64
			for _, ret := range returns {
				varianceSum += math.Pow(ret-meanReturn, 2)
			}
			sharpeRatio := math.Sqrt(varianceSum / float64(len(returns)))

			// Sharpe Ratio Calculation
			// sharpeRatio := (meanReturn - riskFreeRate) / standardDeviation

			// Display Results
			log.Info().Float64("mean_return", meanReturn).
				Float64("sharpe_ratio", sharpeRatio).
				Uint64("fund_id", fund.ID).
				Msg("Sharpe Ratio Calculation")

			if noOfYears == 3 {
				fund.SharpeRatio3Yrs = &sharpeRatio
			} else {
				fund.SharpeRatio5Yrs = &sharpeRatio
			}
			if err = db.Save(&fund).Error; err != nil {
				log.Error().Err(err).Uint64("fund_id", fund.ID).Msg("Failed to fetch report data")
				return err
			}
			return nil
		})
	},
}

func init() {
	rootCmd.AddCommand(sharpeRatioCmd)

	// Here you will define your flags and configuration settings.

	// Cobra supports Persistent Flags which will work for this command
	// and all subcommands, e.g.:
	// sharpeRatioCmd.PersistentFlags().String("foo", "", "A help for foo")

	// Cobra supports local flags which will only run when this command
	// is called directly, e.g.:
	// sharpeRatioCmd.Flags().BoolP("toggle", "t", false, "Help message for toggle")
	sharpeRatioCmd.Flags().IntVar(&noOfYears, "years", 3, "Number of months to consider for sharpe ratio calculation")
}
