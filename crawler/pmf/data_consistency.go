package pmf

import (
	"alpha2/crawler"
	"alpha2/jobs"
	"context"
	"encoding/json"
	"errors"
	"math"
	"strconv"
	"time"

	"github.com/reugn/go-quartz/quartz"
	"github.com/rs/zerolog/log"
	"github.com/samber/lo"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"
)

func init() {
	jobs.RegisterJob("PMSDataConsistencyJob", func() jobs.Job {
		return &PMSDataConsistencyJob{}
	})
}

type PMSDataConsistencyJob struct {
	FundHouseID uint64
}

func (j *PMSDataConsistencyJob) Execute(ctx context.Context) (err error) {

	db := crawler.Conn()

	fundHouse := &crawler.FundManager{}
	err = db.Where(&crawler.FundManager{ID: j.FundHouseID}).Preload("Funds").First(fundHouse).Error
	if err != nil {
		log.Error().Err(err).Msg("Error while getting fund house")
		return err
	}

	resyncReportsForMergedFunds(db, fundHouse.Funds)
	updateDrawdownForFunds(db, fundHouse.Funds, 3)
	updateSharpeRatioForFunds(db, fundHouse.Funds, 3)
	hideFundsIfNoReportsFor3Months(db, fundHouse.Funds)

	return
}

func (j *PMSDataConsistencyJob) SetDescription(s string) {
	err := json.Unmarshal([]byte(s), j)
	if err != nil {
		log.Error().Err(err).Msg("Error while unmarshalling job")
		return
	}
}

func (j *PMSDataConsistencyJob) Description() string {
	data, err := json.Marshal(j)
	if err != nil {
		log.Error().Err(err).Msg("Error while marshalling job")
		return ""
	}
	return string(data)
}

func hideFundsIfNoReportsFor3Months(db *gorm.DB, funds []*crawler.Fund) error {
	// Check if the fund has reports for the last 3 months
	for _, fund := range funds {
		if fund.OtherData != nil && fund.OtherData["original_id"] != "" {
			fund.IsHidden = true
			continue
		}
		reports := []crawler.FundReport{}
		// Get the report for latest 5 months, there is delay in data getting updted in SEBI.
		now := time.Now().AddDate(0, -5, 0)
		err := db.Where(&crawler.FundReport{
			FundID: fund.ID,
		}).
			Where("report_date > ?", now).
			Find(&reports).Error

		if err != nil {
			return err
		}

		fund.IsHidden = len(reports) < 3
	}

	db.Save(&funds)

	return nil
}

func updateDrawdownForFunds(db *gorm.DB, funds []*crawler.Fund, noOfYears int) error {
	for _, fund := range funds {

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
	}

	return nil

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

func updateSharpeRatioForFunds(db *gorm.DB, funds []*crawler.Fund, noOfYears int) error {

	for _, fund := range funds {
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
	}
	return nil
}

func resyncReportsForMergedFunds(db *gorm.DB, funds []*crawler.Fund) error {
	for _, fund := range funds {
		if fund.OtherData == nil || fund.OtherData["original_id"] == "" {
			log.Info().Uint64("fund_id", fund.ID).Msg("No original fund id")
			continue
		}

		originalFundId, err := strconv.ParseUint(fund.OtherData["original_id"], 10, 64)
		if err != nil {
			log.Error().Err(err).Uint64("fund_id", fund.ID).Msg("Error parsing original fund id")
			return err
		}

		reports := []*crawler.FundReport{}
		db.Where(&crawler.FundReport{
			FundID: fund.ID,
		}).FindInBatches(&reports, 100, func(tx *gorm.DB, batch int) error {
			if len(reports) == 0 {
				log.Info().Uint64("fund_id", fund.ID).Msg("No reports found")
				return nil
			}

			lo.ForEach(reports, func(report *crawler.FundReport, index int) {
				report.OtherData["merged_id"] = strconv.FormatUint(fund.ID, 10)
				report.OtherData["priority"] = "low"
				report.FundID = originalFundId
				report.ID = 0
			})

			for _, report := range reports {
				err = db.Model(&crawler.FundReport{}).Clauses(clause.OnConflict{
					DoNothing: true,
				}).Create(report).Error

				if err != nil {
					log.Error().Err(err).Msg("Error saving report")
					return err
				}
			}

			return nil
		})

	}

	return nil
}

func ScheduleDataConsistencyJobIsNotPresent(fundHouseID uint64) error {
	job := &PMSDataConsistencyJob{
		FundHouseID: fundHouseID,
	}
	jd := quartz.NewJobDetail(job, quartz.NewJobKeyWithGroup(strconv.FormatUint(fundHouseID, 10), "PMSDataConsistencyJob"))
	t := quartz.NewRunOnceTrigger(time.Minute * 1)
	err := jobs.Scheduler.ScheduleJob(jd, t)
	if err != nil {
		if errors.Is(err, quartz.ErrJobAlreadyExists) {
			log.Warn().Uint64("fund_house_id", fundHouseID).Msg("PMSDataConsistencyJob alredy present for fund house")
			return nil
		}
		log.Error().Err(err).Msg("Error while scheduling job")
		return err
	}
	return nil
}
