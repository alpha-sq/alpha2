/*
Copyright © 2025 NAME HERE <EMAIL ADDRESS>
*/
package cmd

import (
	"alpha2/crawler"
	"alpha2/jobs"
	"fmt"

	"github.com/spf13/cobra"
	"gorm.io/gorm"
)

// flushCmd represents the flush command
var flushCmd = &cobra.Command{
	Use:   "flush",
	Short: "flushes all tables",
	Long:  `This command truncates all tables in the database and restarts the identity sequence.`,
	Run: func(cmd *cobra.Command, args []string) {
		var tables = []any{
			&crawler.FundManager{},
			&crawler.Fund{},
			&crawler.FundReport{},
			&crawler.CrawlerEvent{},
			&crawler.FundXFundManagers{},
			&jobs.ScheduledJob{},
		}
		db := crawler.Conn()
		for _, table := range tables {
			stmt := &gorm.Statement{DB: db}
			stmt.Parse(table)
			db.Exec(fmt.Sprintf("TRUNCATE TABLE %s RESTART IDENTITY CASCADE", stmt.Schema.Table))
		}
	},
}

func init() {
	rootCmd.AddCommand(flushCmd)

	// Here you will define your flags and configuration settings.

	// Cobra supports Persistent Flags which will work for this command
	// and all subcommands, e.g.:
	// flushCmd.PersistentFlags().String("foo", "", "A help for foo")

	// Cobra supports local flags which will only run when this command
	// is called directly, e.g.:
	// flushCmd.Flags().BoolP("toggle", "t", false, "Help message for toggle")
}
