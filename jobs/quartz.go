package jobs

import (
	"alpha2/crawler"
	"encoding/json"
	"errors"
	"sync"
	"time"

	"github.com/reugn/go-quartz/quartz"
	"gorm.io/gorm"
)

var Scheduler quartz.Scheduler

func Init() {
	jobQueue := NewGormJobQueue(crawler.Conn())
	Scheduler, _ = quartz.NewStdScheduler(
		quartz.WithQueue(jobQueue, &sync.Mutex{}),
		quartz.WithLogger(&JobZeroLogger{}),
		quartz.WithOutdatedThreshold(time.Hour*24*7),
		quartz.WithRetryInterval(time.Minute*5),
		quartz.WithWorkerLimit(10),
	)
}

// GormJobQueue implements the JobQueue interface using Gorm and PostgreSQL.
type GormJobQueue struct {
	db  *gorm.DB
	mtx sync.Mutex
}

// NewGormJobQueue initializes a new GormJobQueue.
func NewGormJobQueue(db *gorm.DB) *GormJobQueue {
	return &GormJobQueue{db: db}
}

// Push inserts a new scheduled job into the queue.
func (gq *GormJobQueue) Push(job quartz.ScheduledJob) error {
	gq.mtx.Lock()
	defer gq.mtx.Unlock()

	options := job.JobDetail().Options()
	jobData, err := json.Marshal(options) // Replace with serialized job options if needed
	if err != nil {
		return err
	}
	jobDetail := job.JobDetail()
	scheduledJob := ScheduledJob{
		JobKey:             jobDetail.JobKey().String(),
		JobName:            jobDetail.JobKey().Name(),
		JobGroup:           jobDetail.JobKey().Group(),
		JobDescription:     jobDetail.Job().Description(),
		JobData:            string(jobData),   // Replace with serialized job data if needed
		NxtRunTime:         job.NextRunTime(), // int64 Unix timestamp
		JobStatus:          "scheduled",
		TriggerDescription: job.Trigger().Description(),
	}

	// Check if the job already exists
	var existingJob ScheduledJob
	if err := gq.db.Where("job_key = ?", scheduledJob.JobKey).First(&existingJob).Error; err == nil {
		if jobDetail.Options().Replace {
			// Update the existing job
			return gq.db.Model(&existingJob).Updates(scheduledJob).Error
		}
		return errors.New("job already exists")
	}

	// Insert the new job
	return gq.db.Create(&scheduledJob).Error
}

// Pop removes and returns the next scheduled job from the queue.
func (gq *GormJobQueue) Pop() (quartz.ScheduledJob, error) {
	gq.mtx.Lock()
	defer gq.mtx.Unlock()

	var scheduledJob ScheduledJob
	if err := gq.db.Order("nxt_run_time").First(&scheduledJob).Error; err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return nil, quartz.ErrQueueEmpty
		}
		return nil, err
	}

	// Remove the job from the queue
	if err := gq.db.Delete(&scheduledJob).Error; err != nil {
		return nil, err
	}

	return &scheduledJob, nil
}

// Head returns the first scheduled job without removing it from the queue.
func (gq *GormJobQueue) Head() (quartz.ScheduledJob, error) {
	gq.mtx.Lock()
	defer gq.mtx.Unlock()

	var scheduledJob ScheduledJob
	if err := gq.db.Order("nxt_run_time").First(&scheduledJob).Error; err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return nil, quartz.ErrQueueEmpty
		}
		return nil, err
	}

	return &scheduledJob, nil
}

// Get returns the scheduled job with the specified key without removing it from the queue.
func (gq *GormJobQueue) Get(jobKey *quartz.JobKey) (quartz.ScheduledJob, error) {
	gq.mtx.Lock()
	defer gq.mtx.Unlock()

	var scheduledJob ScheduledJob
	if err := gq.db.Where("job_key = ?", jobKey.String()).First(&scheduledJob).Error; err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return nil, quartz.ErrJobNotFound
		}
		return nil, err
	}

	return &scheduledJob, nil
}

// Remove removes and returns the scheduled job with the specified key.
func (gq *GormJobQueue) Remove(jobKey *quartz.JobKey) (quartz.ScheduledJob, error) {
	gq.mtx.Lock()
	defer gq.mtx.Unlock()

	var scheduledJob ScheduledJob
	if err := gq.db.Where("job_key = ?", jobKey.String()).First(&scheduledJob).Error; err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return nil, quartz.ErrJobNotFound
		}
		return nil, err
	}

	// Remove the job from the queue
	if err := gq.db.Delete(&scheduledJob).Error; err != nil {
		return nil, err
	}

	return &scheduledJob, nil
}

// ScheduledJobs returns a slice of scheduled jobs in the queue.
func (gq *GormJobQueue) ScheduledJobs(matchers []quartz.Matcher[quartz.ScheduledJob]) ([]quartz.ScheduledJob, error) {
	gq.mtx.Lock()
	defer gq.mtx.Unlock()

	var scheduledJobs []ScheduledJob
	if err := gq.db.Find(&scheduledJobs).Error; err != nil {
		return nil, err
	}

	var result []quartz.ScheduledJob
	for _, scheduledJob := range scheduledJobs {
		// Apply matchers
		matches := true
		for _, matcher := range matchers {
			if !matcher.IsMatch(&scheduledJob) {
				matches = false
				break
			}
		}
		if matches {
			result = append(result, &scheduledJob)
		}
	}

	return result, nil
}

// Size returns the size of the job queue.
func (gq *GormJobQueue) Size() (int, error) {
	gq.mtx.Lock()
	defer gq.mtx.Unlock()

	var count int64
	if err := gq.db.Model(&ScheduledJob{}).Count(&count).Error; err != nil {
		return 0, err
	}
	return int(count), nil
}

// Clear clears the job queue.
func (gq *GormJobQueue) Clear() error {
	gq.mtx.Lock()
	defer gq.mtx.Unlock()

	return gq.db.Where("1 = 1").Delete(&ScheduledJob{}).Error
}
