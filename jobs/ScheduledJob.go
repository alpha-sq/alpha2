package jobs

import (
	"context"
	"strings"
	"time"

	"github.com/puzpuzpuz/xsync/v3"
	"github.com/reugn/go-quartz/quartz"
	"gorm.io/gorm"
)

type ScheduledJob struct {
	gorm.Model
	JobKey             string `gorm:"not null"`  // Unique identifier for the job
	JobName            string `gorm:"not null"`  // Name of the job
	JobGroup           string `gorm:"not null"`  // Group of the job
	JobDescription     string `gorm:"not null"`  // Group of the job
	JobData            string `gorm:"type:text"` // Additional job data
	NxtRunTime         int64  `gorm:"not null"`  // Next execution time as Unix timestamp
	JobStatus          string `gorm:"not null"`  // Status of the job (e.g., "scheduled", "executed")
	TriggerDescription string `gorm:"not null"`  // Status of the job (e.g., "scheduled", "executed")
}

type Job interface {
	// Execute is called by a Scheduler when the Trigger associated
	// with this job fires.
	Execute(context.Context) error

	// Description returns the description of the Job.
	Description() string
	SetDescription(string)
}

// NextRunTime implements quartz.ScheduledJob.
func (sj *ScheduledJob) NextRunTime() int64 {
	return sj.NxtRunTime
}

// Trigger implements quartz.ScheduledJob.
func (sj *ScheduledJob) Trigger() quartz.Trigger {
	triggerOpts := strings.Split(sj.TriggerDescription, quartz.Sep)
	interval, err := time.ParseDuration(triggerOpts[1])
	if err != nil {
		return quartz.NewRunOnceTrigger(time.Second * 5)
	}
	if strings.Contains(triggerOpts[0], "SimpleTrigger") {
		return quartz.NewSimpleTrigger(interval)
	} else if strings.Contains(triggerOpts[0], "CronTrigger") {
		t, _ := quartz.NewCronTrigger(triggerOpts[1])
		return t
	} else {

		return &quartz.RunOnceTrigger{
			Delay:   interval,
			Expired: triggerOpts[2] == "expired",
		}
	}
}

func (sj *ScheduledJob) JobDetail() *quartz.JobDetail {
	j := GetJob(sj.JobGroup)
	j.SetDescription(sj.JobDescription)
	job := quartz.NewJobDetailWithOptions(
		j,
		quartz.NewJobKeyWithGroup(sj.JobName, sj.JobGroup),
		&quartz.JobDetailOptions{
			MaxRetries:    10,
			RetryInterval: time.Minute * 5,
			Replace:       false,
			Suspended:     false,
		},
	)
	return job
}

var JobRegistry *xsync.MapOf[string, func() Job]

func init() {
	JobRegistry = xsync.NewMapOf[string, func() Job]()
}

func RegisterJob(group string, supplier func() Job) {
	JobRegistry.Store(group, supplier)
}

func GetJob(group string) Job {
	supplier, _ := JobRegistry.Load(group)
	return supplier()
}
