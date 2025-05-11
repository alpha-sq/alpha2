package pmf

import (
	"alpha2/crawler"
	"alpha2/jobs"
	"context"
	"crypto/rand"
	"encoding/json"
	"fmt"
	"math/big"
	"time"

	"github.com/reugn/go-quartz/quartz"
	"github.com/rs/zerolog/log"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"
)

func init() {
	jobs.RegisterJob("CrawlPMFFunds", func() jobs.Job {
		return &CrawlPMFFunds{}
	})
	jobs.RegisterJob("PMFInit", func() jobs.Job {
		return &PMFInit{}
	})
}

type CrawlPMFFunds struct {
	UID      string
	ForDate  string
	SkipNext bool
}

func (j *CrawlPMFFunds) Execute(ctx context.Context) (err error) {
	forDate, _ := time.Parse(time.DateOnly, j.ForDate)

	crwl := NewPMFCrawler()
	crwl.CrawlFundWithManager(j.UID, &forDate, func(funds []*crawler.Fund) {
		if crwl.err != nil {
			return
		}
		db := crawler.Conn()
		db.Transaction(func(tx *gorm.DB) error {
			for _, fund := range funds {
				fund.FundManagers[0].RefreshedDate = &forDate

				tx := db.Model(&crawler.FundManager{}).Clauses(clause.OnConflict{
					Columns: []clause.Column{{Name: "id"}},
					Where: clause.Where{Exprs: []clause.Expression{
						clause.And(
							clause.Eq{Column: "fund_managers.id", Value: fund.FundManagers[0].ID},
							clause.Lt{Column: "fund_managers.refreshed_date", Value: forDate},
						),
					}},
					DoUpdates: clause.AssignmentColumns([]string{"name", "email", "contact", "address", "total_no_of_client", "other_data", "total_aum",
						"refreshed_date"}),
				}).Omit("Funds").Create(fund.FundManagers[0])
				if tx.Error != nil {
					jsonfund, _ := json.Marshal(fund)
					log.Error().Err(tx.Error).RawJSON("fund", jsonfund).Msg("Error while saving funds")
					err = tx.Error
					return tx.Error
				}

				tx = db.Model(&crawler.Fund{}).Clauses(clause.OnConflict{
					Columns:   []clause.Column{{Name: "id"}},
					DoNothing: true,
				}).Omit("FundReports").Create(fund)
				if tx.Error != nil {
					jsonfund, _ := json.Marshal(fund)
					log.Error().Err(tx.Error).RawJSON("fund", jsonfund).Msg("Error while saving funds")
					err = tx.Error
					return tx.Error
				}

				for _, fundReport := range fund.FundReports {
					fundReport.FundID = fund.ID
					tx = db.Model(&crawler.FundReport{}).Clauses(clause.OnConflict{
						Columns:   []clause.Column{{Name: "fund_id"}, {Name: "report_date"}},
						UpdateAll: true,
					}).Create(fundReport)
					if tx.Error != nil {
						jsonfund, _ := json.Marshal(fund)
						log.Error().Err(tx.Error).RawJSON("fund", jsonfund).Msg("Error while saving funds")
						err = tx.Error
						return tx.Error
					}
				}

				err = ScheduleDataConsistencyJobIsNotPresent(fund.FundManagers[0].ID)
				if err != nil {
					jsonfund, _ := json.Marshal(fund)
					log.Error().Err(err).RawJSON("fund", jsonfund).Msg("Error while scheduling job")
					return err
				}
			}

			if j.SkipNext {
				return nil
			}

			nxtDate := forDate.AddDate(0, 1, 0)

			var triggerDur time.Duration

			now := time.Now().AddDate(0, 0, -time.Now().Day())
			if nxtDate.Before(now) {
				triggerDur = time.Second * 1
			} else {
				triggerDur = time.Until(NextTimeToRunJob())
			}

			job := &CrawlPMFFunds{
				UID:     j.UID,
				ForDate: nxtDate.Format(time.DateOnly),
			}

			key := fmt.Sprintf("%s-%s-%s", j.UID, nxtDate.Format(time.DateOnly), RandomString(3))
			jobDetail := quartz.NewJobDetailWithOptions(
				job, quartz.NewJobKeyWithGroup(key, "CrawlPMFFunds"),
				&quartz.JobDetailOptions{
					MaxRetries:    10,
					RetryInterval: time.Minute * 5,
					Replace:       false,
					Suspended:     false,
				},
			)

			err = jobs.Scheduler.ScheduleJob(jobDetail, quartz.NewRunOnceTrigger(triggerDur))

			return err
		})
	})
	if crwl.err != nil {
		return crwl.err
	}

	return err
}

func (j *CrawlPMFFunds) SetDescription(s string) {
	err := json.Unmarshal([]byte(s), j)
	if err != nil {
		log.Error().Err(err).Msg("Error while unmarshalling job")
		return
	}
}

func (j *CrawlPMFFunds) Description() string {
	data, err := json.Marshal(j)
	if err != nil {
		log.Error().Err(err).Msg("Error while marshalling job")
		return ""
	}
	return string(data)
}

func NextTimeToRunJob() time.Time {
	// Get the current time
	now := time.Now()

	// Calculate the first day of the current month
	firstDayOfCurrentMonth := time.Date(now.Year(), now.Month(), 21, 0, 0, 0, 0, now.Location())

	// Add one month to get the first day of the next month
	firstDayOfNextMonth := firstDayOfCurrentMonth.AddDate(0, 1, 0)

	return firstDayOfNextMonth
}

type PMFInit struct{}

func (j *PMFInit) Execute(ctx context.Context) (err error) {
	UIDs := CrawlFundManagarIDs()
	for _, UID := range UIDs {
		job := &CrawlPMFFunds{
			UID:     UID,
			ForDate: "2021-01-01",
		}

		key := fmt.Sprintf("%s-%s-%s", job.UID, job.ForDate, RandomString(3))
		jobDetail := quartz.NewJobDetailWithOptions(
			job, quartz.NewJobKeyWithGroup(key, "CrawlPMFFunds"),
			&quartz.JobDetailOptions{
				MaxRetries:    10,
				RetryInterval: time.Minute * 5,
				Replace:       false,
				Suspended:     false,
			},
		)

		err = jobs.Scheduler.ScheduleJob(jobDetail, quartz.NewRunOnceTrigger(time.Second*1))
		if err != nil {
			return err
		}
	}
	return nil
}
func (j *PMFInit) SetDescription(s string) {
}

func (p *PMFInit) Description() string {
	return "PMFInit"
}

const charset = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789"

func RandomString(length int) string {
	result := make([]byte, length)
	for i := range result {
		num, err := rand.Int(rand.Reader, big.NewInt(int64(len(charset))))
		if err != nil {
			return ""
		}
		result[i] = charset[num.Int64()]
	}
	return string(result)
}
