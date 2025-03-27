/*
Copyright © 2025 NAME HERE <EMAIL ADDRESS>
*/
package cmd

import (
	"alpha2/crawler"

	"github.com/rs/zerolog/log"
	"github.com/spf13/cobra"
	"gorm.io/gorm"
)

// ResolveDuplicateCmd represents the ResolveDuplicate command
var ResolveDuplicateCmd = &cobra.Command{
	Use:   "ResolveDuplicate",
	Short: "A brief description of your command",
	Long: `A longer description that spans multiple lines and likely contains examples
and usage of using your command. For example:

Cobra is a CLI library for Go that empowers applications.
This application is a tool to generate the needed files
to quickly create a Cobra application.`,
	Run: func(cmd *cobra.Command, args []string) {
		db := crawler.Conn()

		fms := []*crawler.FundManager{}
		err := db.FindInBatches(&fms, 1, func(tx *gorm.DB, batch int) error {
			fm := fms[0]
			dupList, err := findDuplicateFundsUsingScore(db, fm.ID)
			if err != nil {
				return err
			}
			log.Info().Uint64("fund_manager", fm.ID).Int("count", len(dupList)).Msg("duplicates cehck result for fund manager: " + fm.Name)
			for _, result := range dupList {
				result.FundManagerID = fm.ID
			}
			onDuplicateData(db, dupList)
			return nil
		}).Error
		if err != nil {
			log.Error().Err(err).Msg("Error during migration")
		}

		dupList, err := findDuplicateFundsWithSpace(db)
		if err != nil {
			log.Error().Err(err).Msg("Error during migration")
			return
		}
		log.Info().Int("count", len(dupList)).Msg("findDuplicateFundsWithSpace")

		err = onDuplicateData(db, dupList)
		if err != nil {
			log.Error().Err(err).Msg("Error during migration")
			return
		}
		log.Info().Msg("Migration completed successfully")
	},
}

func onDuplicateData(db *gorm.DB, dupList []*FundDuplicate) error {
	if len(dupList) == 0 {
		return nil
	}

	idVsDupID := make(map[uint64]uint64)
	for _, result := range dupList {
		if prID, ok := idVsDupID[result.ID]; ok {
			result.ID = prID
		} else {
			idVsDupID[result.DuplicateID] = result.ID
		}
		log.Info().Uint64("fund_id", result.ID).Uint64("duplicate_fund_id", result.DuplicateID).Str("name", result.Name).Str("duplicate_name", result.DuplicateName).Msg("Migrating duplicate Fund Data")
		db.Find(&crawler.Fund{ID: result.ID})
		err := updateReport(db, result)
		if err != nil {
			return err
		}

		err = db.Where(&crawler.FundXFundManagers{
			FundID:        result.DuplicateID,
			FundManagerID: result.FundManagerID,
		}).Delete(&crawler.FundXFundManagers{}).Error
		if err != nil {
			return err
		}
		err = db.Delete(&crawler.Fund{}, result.DuplicateID).Error
		if err != nil {
			return err
		}
	}

	return nil
}

func updateReport(db *gorm.DB, dup *FundDuplicate) error {
	updateQuery := `
	UPDATE fund_reports
	SET fund_id = ?
	WHERE fund_id = ?;`
	err := db.Exec(updateQuery, dup.ID, dup.DuplicateID).Error
	return err
}

type FundDuplicate struct {
	ID            uint64
	Name          string
	DuplicateID   uint64
	DuplicateName string
	FundManagerID uint64
	Score         float64
}

func findDuplicateFundsUsingScore(db *gorm.DB, fundManagerID uint64) ([]*FundDuplicate, error) {
	var results []*FundDuplicate

	query := `
        SELECT f1.id, f1.name, f2.id AS duplicate_id, f2.name AS duplicate_name,
               similarity(f1.name, f2.name) AS score
        FROM funds f1
        JOIN funds f2 
            ON f1.id < f2.id  -- Avoid self-joins and duplicate comparisons
            AND similarity(f1.name, f2.name) > 0.9  -- Adjust threshold
        WHERE f1.id IN (
            SELECT fxfm.fund_id FROM fund_x_fund_managers fxfm WHERE fxfm.fund_manager_id = ?
        ) AND f2.id IN (
		    SELECT fxfm.fund_id FROM fund_x_fund_managers fxfm WHERE fxfm.fund_manager_id = ?
		)
        ORDER BY score DESC;
    `

	err := db.Raw(query, fundManagerID, fundManagerID).Scan(&results).Error
	if err != nil {
		return nil, err
	}

	return results, nil
}

func findDuplicateFundsWithSpace(db *gorm.DB) ([]*FundDuplicate, error) {
	var results []*FundDuplicate

	query := `
with fund1 as (
select
	replace(name,
	' ',
	'') as name,
	name as r_name,
	id
from
	funds)
	select
	f1.r_name as name,
	f2.r_name as duplicate_name,
	fxfm1.fund_manager_id,
	f1.id,
	f2.id as duplicate_id
from
	fund1 as f1
inner join fund1 f2 on
	f1."name" = f2."name"
	and f1.id < f2.id
inner join fund_x_fund_managers fxfm1 on
	fxfm1.fund_id = f1.id
inner join fund_x_fund_managers fxfm2 on
	fxfm2.fund_id = f2.id
	and fxfm1.fund_manager_id = fxfm2.fund_manager_id
    `
	err := db.Raw(query).Scan(&results).Error
	if err != nil {
		return nil, err
	}

	return results, nil
}

func init() {
	rootCmd.AddCommand(ResolveDuplicateCmd)

	// Here you will define your flags and configuration settings.

	// Cobra supports Persistent Flags which will work for this command
	// and all subcommands, e.g.:
	// ResolveDuplicateCmd.PersistentFlags().String("foo", "", "A help for foo")

	// Cobra supports local flags which will only run when this command
	// is called directly, e.g.:
	// ResolveDuplicateCmd.Flags().BoolP("toggle", "t", false, "Help message for toggle")
}
