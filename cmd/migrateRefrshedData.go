/*
Copyright © 2025 NAME HERE <EMAIL ADDRESS>
*/
package cmd

import (
	"alpha2/crawler"
	"fmt"
	"time"

	"github.com/spf13/cobra"
)

// migrateRefrshedDataCmd represents the migrateRefrshedData command
var migrateRefrshedDataCmd = &cobra.Command{
	Use:   "migrateRefrshedData",
	Short: "A brief description of your command",
	Long: `A longer description that spans multiple lines and likely contains examples
and usage of using your command. For example:

Cobra is a CLI library for Go that empowers applications.
This application is a tool to generate the needed files
to quickly create a Cobra application.`,
	Run: func(cmd *cobra.Command, args []string) {
		fmt.Println("migrateRefrshedData called")
		db := crawler.Conn()

		db.Model(&crawler.FundManager{}).Where("refreshed_date IS NULL").Update("refreshed_date", time.Date(2025, time.March, 1, 0, 0, 0, 0, time.UTC))
	},
}

func init() {
	rootCmd.AddCommand(migrateRefrshedDataCmd)

	// Here you will define your flags and configuration settings.

	// Cobra supports Persistent Flags which will work for this command
	// and all subcommands, e.g.:
	// migrateRefrshedDataCmd.PersistentFlags().String("foo", "", "A help for foo")

	// Cobra supports local flags which will only run when this command
	// is called directly, e.g.:
	// migrateRefrshedDataCmd.Flags().BoolP("toggle", "t", false, "Help message for toggle")
}
