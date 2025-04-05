/*
Copyright © 2025 NAME HERE <EMAIL ADDRESS>
*/
package cmd

import (
	"alpha2/crawler"
	"encoding/csv"
	"errors"
	"os"
	"slices"
	"strconv"

	"github.com/rs/zerolog/log"
	"github.com/samber/lo"
	"github.com/spf13/cobra"
	"gorm.io/gorm"
)

// mergeFundCmd represents the mergeFund command
var mergeFundCmd = &cobra.Command{
	Use:   "mergeFund",
	Short: "A brief description of your command",
	Long: `A longer description that spans multiple lines and likely contains examples
and usage of using your command. For example:

Cobra is a CLI library for Go that empowers applications.
This application is a tool to generate the needed files
to quickly create a Cobra application.`,
	Run: func(cmd *cobra.Command, args []string) {
		file, err := os.Open("./static/funds.csv")
		if err != nil {
			log.Fatal().Err(err).Msg("Error opening file")
		}
		defer file.Close()

		// Create a new CSV reader
		reader := csv.NewReader(file)

		// Read all records
		records, err := reader.ReadAll()
		if err != nil {
			log.Fatal().Err(err).Msg("Error reading CSV")
		}

		slices.SortFunc(records, func(r1, r2 []string) int {
			if len(r1) >= 4 && r1[3] == "1" {
				return -1
			}
			if len(r2) >= 4 && r2[3] == "1" {
				return 1
			}
			return 0
		})

		db := crawler.Conn()
		// Print each record
		for idx, record := range records {
			if record[1] == "" || record[2] == "" {
				continue
			}
			if len(record) > 4 {
				log.Error().Err(err).Str("fund", record[1]).Msg("More Cols")
				return
			}

			fund := &crawler.Fund{}
			err := db.Where(&crawler.Fund{Name: record[1]}).
				Preload("FundManagers").
				First(fund).Error
			if err != nil {
				log.Error().Err(err).Str("fund", record[1]).Msg("Error finding fund")
				continue
			}

			if fund.OtherData == nil {
				fund.OtherData = crawler.JSONB{}
			}
			fund.OtherData["label"] = record[2]
			fund.OtherData["perf"] = strconv.Itoa(idx)

			originalFund := &crawler.Fund{}
			err = db.Model(&crawler.Fund{}).
				Where("other_data->>'label' = ? and is_hidden = false", record[2]).
				Preload("FundManagers").
				Find(originalFund).Error
			if errors.Is(err, gorm.ErrRecordNotFound) || originalFund.ID == 0 {
				originalFund = nil
			} else if err != nil {
				log.Error().Err(err).Str("fund", record[1]).Msg("Error finding original fund")
				continue
			} else if len(originalFund.FundManagers) > 0 && len(fund.FundManagers) > 0 && originalFund.FundManagers[0].ID != fund.FundManagers[0].ID {
				log.Info().Str("fund", record[1]).Msg("Fund managers do not match")
				originalFund = nil
			}

			if originalFund != nil {
				fund.OtherData["original_id"] = strconv.FormatUint(originalFund.ID, 10)
				fund.IsHidden = true
			}

			err = db.Save(fund).Error
			if err != nil {
				log.Error().Err(err).Str("fund", record[1]).Msg("Error saving fund")
				continue
			}
		}

		funds := []crawler.Fund{}
		db.Where(&crawler.Fund{IsHidden: false, Type: "PMF"}).FindInBatches(&funds, 1, func(tx *gorm.DB, batch int) error {

			fund := funds[0]
			duplicatefunds := []crawler.Fund{}
			err := tx.Where("other_data->>'original_id' = ?", strconv.FormatUint(fund.ID, 10)).Find(&duplicatefunds).Error
			if err != nil {
				log.Error().Err(err).Str("fund", fund.Name).Msg("Error finding duplicate funds")
				return err
			}

			if len(duplicatefunds) == 0 {
				log.Info().Str("fund", fund.Name).Msg("No duplicate funds found")
				return nil
			}

			log.Info().Str("fund", fund.Name).Msg("Duplicate funds found")

			duplicateFundReports := []crawler.FundReport{}
			duplicateFundIDs := lo.Map(duplicatefunds, func(fund crawler.Fund, index int) uint64 {
				return fund.ID
			})
			tx.Where("fund_id in ?", duplicateFundIDs).Find(&duplicateFundReports)
			if len(duplicateFundReports) == 0 {
				log.Info().Str("fund", fund.Name).Msg("No duplicate fund reports found")
				return nil
			}

			lo.ForEach(duplicateFundReports, func(report crawler.FundReport, index int) {
				report.OtherData["merged_id"] = strconv.FormatUint(report.FundID, 10)
				report.FundID = fund.ID
			})

			err = db.Model(&crawler.FundReport{}).Save(duplicateFundReports).Error
			if err != nil {
				log.Error().Err(err).Str("fund", fund.Name).Msg("Error updating fund reports")
				return err
			}

			return nil
		})
	},
}

func init() {
	rootCmd.AddCommand(mergeFundCmd)

	// Here you will define your flags and configuration settings.

	// Cobra supports Persistent Flags which will work for this command
	// and all subcommands, e.g.:
	// mergeFundCmd.PersistentFlags().String("foo", "", "A help for foo")

	// Cobra supports local flags which will only run when this command
	// is called directly, e.g.:
	// mergeFundCmd.Flags().BoolP("toggle", "t", false, "Help message for toggle")
}
