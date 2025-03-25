/*
Copyright © 2025 NAME HERE <EMAIL ADDRESS>
*/
package cmd

import (
	"alpha2/crawler"
	"alpha2/crawler/mf"
	"alpha2/jobs"

	"github.com/rs/zerolog/log"
	"github.com/spf13/cobra"
)

// autoMigrateCmd represents the autoMigrate command
var autoMigrateCmd = &cobra.Command{
	Use:   "autoMigrate",
	Short: "Auto migrate the database",
	Run: func(cmd *cobra.Command, args []string) {
		var err error
		db := crawler.Conn()

		if err = db.AutoMigrate(&crawler.FundManager{}); err != nil {
			log.Panic().Err(err).Msg("Error migrating FundManager")
		}
		if err = db.AutoMigrate(&crawler.Fund{}); err != nil {
			log.Panic().Err(err).Msg("Error migrating Fund")
		}
		if err = db.AutoMigrate(&crawler.FundReport{}); err != nil {
			log.Panic().Err(err).Msg("Error migrating FundFundReportManager")
		}
		if err = db.AutoMigrate(&crawler.CrawlerEvent{}); err != nil {
			log.Panic().Err(err).Msg("Error migrating CrawlerEvent")
		}
		if err = db.SetupJoinTable(&crawler.FundManager{}, "Funds", &crawler.FundXFundManagers{}); err != nil {
			log.Panic().Err(err).Msg("Error migrating FundXFundManagers")
		}
		if err = db.AutoMigrate(&mf.MutualFundData{}); err != nil {
			log.Panic().Err(err).Msg("Error migrating MutualFundData")
		}
		if err = db.AutoMigrate(&mf.MutualFundNav{}); err != nil {
			log.Panic().Err(err).Msg("Error migrating MutualFundNav")
		}

		if err = db.Exec("CREATE EXTENSION IF NOT EXISTS pg_trgm;").Error; err != nil {
			log.Panic().Err(err).Msg("Error CREATE EXTENSION pg_trgm")
		}

		if err = db.AutoMigrate(&jobs.ScheduledJob{}); err != nil {
			log.Panic().Err(err).Msg("Error migrating CrawlerEvent")
		}
	},
}

func init() {
	rootCmd.AddCommand(autoMigrateCmd)

	// Here you will define your flags and configuration settings.

	// Cobra supports Persistent Flags which will work for this command
	// and all subcommands, e.g.:
	// autoMigrateCmd.PersistentFlags().String("foo", "", "A help for foo")

	// Cobra supports local flags which will only run when this command
	// is called directly, e.g.:
	// autoMigrateCmd.Flags().BoolP("toggle", "t", false, "Help message for toggle")
}
