package pmf

import (
	"alpha2/crawler"
	"alpha2/jobs"
	"context"
	"encoding/json"
	"fmt"
	"strings"
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
	UID     string
	ForDate string
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
				tx := db.Model(&crawler.FundManager{}).Clauses(clause.OnConflict{
					Columns:   []clause.Column{{Name: "id"}},
					DoUpdates: clause.AssignmentColumns([]string{"name", "email", "contact", "address", "total_no_of_client", "other_data", "total_aum"}),
				}).Omit("Funds").Create(fund.FundManagers[0])
				if tx.Error != nil {
					jsonfund, _ := json.Marshal(fund)
					log.Error().Err(tx.Error).RawJSON("fund", jsonfund).Msg("Error while saving funds")
					err = tx.Error
					return tx.Error
				}

				tx = db.Model(&crawler.Fund{}).Clauses(clause.OnConflict{
					Columns:   []clause.Column{{Name: "id"}},
					UpdateAll: true,
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

			key := fmt.Sprintf("%s-%s", j.UID, nxtDate.Format(time.DateOnly))
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
	r := strings.Split(s, quartz.Sep)
	j.UID = r[0]
	j.ForDate = r[1]
}
func (j *CrawlPMFFunds) Description() string {
	return fmt.Sprintf("%s%s%s", j.UID, quartz.Sep, j.ForDate)
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

		key := fmt.Sprintf("%s-%s", job.UID, job.ForDate)
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
