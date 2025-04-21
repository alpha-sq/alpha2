/*
Copyright © 2025 NAME HERE <EMAIL ADDRESS>
*/
package cmd

import (
	"alpha2/crawler"

	"github.com/spf13/cobra"
	"gorm.io/gorm"
)

// migrateManagerDataCmd represents the migrateManagerData command
var migrateManagerDataCmd = &cobra.Command{
	Use:   "migrateManagerData",
	Short: "A brief description of your command",
	Long: `A longer description that spans multiple lines and likely contains examples
and usage of using your command. For example:

Cobra is a CLI library for Go that empowers applications.
This application is a tool to generate the needed files
to quickly create a Cobra application.`,
	Run: func(cmd *cobra.Command, args []string) {
		db := crawler.Conn()

		var fundHouses []*crawler.FundManager
		db.Model(crawler.FundManager{}).Preload("Managers").FindInBatches(&fundHouses, 100, func(tx *gorm.DB, batch int) error {

			managers := make([]*crawler.Manager, 0)
			for _, fundHouse := range fundHouses {

				if len(fundHouse.Managers) > 0 {
					continue
				}

				managers = append(managers, &crawler.Manager{
					FundManagerID: fundHouse.ID,
					Name:          fundHouse.Name,
					Title:         "Principal Officer",
					Email:         fundHouse.Email,
					Contact:       fundHouse.Contact,
				})

				if fundHouse.OtherData != nil && fundHouse.OtherData["ComplianceOfficer"] != "" {
					managers = append(managers, &crawler.Manager{
						FundManagerID: fundHouse.ID,
						Name:          fundHouse.Name,
						Title:         "Compliance Officer",
						Email:         fundHouse.OtherData["ComplianceOfficerEmail"],
					})
				}

			}

			if err := db.Create(&managers).Error; err != nil {
				return err
			}

			return nil
		})
	},
}

func init() {
	rootCmd.AddCommand(migrateManagerDataCmd)

	// Here you will define your flags and configuration settings.

	// Cobra supports Persistent Flags which will work for this command
	// and all subcommands, e.g.:
	// migrateManagerDataCmd.PersistentFlags().String("foo", "", "A help for foo")

	// Cobra supports local flags which will only run when this command
	// is called directly, e.g.:
	// migrateManagerDataCmd.Flags().BoolP("toggle", "t", false, "Help message for toggle")
}
