package mf

import (
	"alpha2/crawler"
	"alpha2/jobs"
	"context"
	"fmt"
	"strconv"
	"time"

	"github.com/reugn/go-quartz/quartz"
	"gorm.io/gorm/clause"
)

func init() {
	jobs.RegisterJob("MFSync", func() jobs.Job {
		return &MFSync{}
	})
	jobs.RegisterJob("MFNavSync", func() jobs.Job {
		return &MFNavSync{}
	})
}

type MFSync struct {
}

func (j *MFSync) Execute(ctx context.Context) error {
	mfCrawler := NewMutualFundCrawler()
	funds, err := mfCrawler.CrawlFundMeta()
	if err != nil {
		return err
	}

	if len(funds) == 0 {
		return nil
	}
	db := crawler.Conn()
	db.Clauses(clause.OnConflict{
		DoNothing: true,
	}).Save(&funds)
	for idx, fund := range funds {
		job := &MFNavSync{
			FundID: fund.ID,
		}

		nxtDate := time.Now().AddDate(0, 0, -time.Now().Day())
		key := fmt.Sprintf("%d-%s", job.FundID, nxtDate.Format(time.DateOnly))
		jobDetail := quartz.NewJobDetailWithOptions(
			job, quartz.NewJobKeyWithGroup(key, "MFNavSync"),
			&quartz.JobDetailOptions{
				MaxRetries:    10,
				RetryInterval: time.Minute * 5,
				Replace:       false,
				Suspended:     false,
			},
		)

		err = jobs.Scheduler.ScheduleJob(jobDetail, quartz.NewRunOnceTrigger(time.Minute*time.Duration(idx)))
		if err != nil {
			return err
		}
	}

	return nil
}

func (j *MFSync) SetDescription(s string) {
}

func (p *MFSync) Description() string {
	return ""
}

type MFNavSync struct {
	FundID uint
}

func (m *MFNavSync) Execute(ctx context.Context) error {
	mfCrawler := NewMutualFundCrawler()

	db := crawler.Conn()
	var res MutualFundData
	db.Where("id = ?", m.FundID).Find(&res)
	navs, err := mfCrawler.CrawlFundNav(&res)
	if err != nil {
		return err
	}
	if len(navs) == 0 {
		db.Save(&crawler.CrawlerEvent{
			Data: crawler.JSONB{"FundID": strconv.Itoa(int(m.FundID)), "error": "No NAVs found"},
		})
		return nil
	}
	db.Clauses(clause.OnConflict{
		DoNothing: true,
	}).Save(&navs)

	return nil
}

func (m *MFNavSync) SetDescription(s string) {
	f, _ := strconv.Atoi(s)
	m.FundID = uint(f)
}

func (m *MFNavSync) Description() string {
	return strconv.Itoa(int(m.FundID))
}
