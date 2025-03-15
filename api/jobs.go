package api

import (
	"alpha2/jobs"
	"net/http"
	"time"

	"github.com/go-chi/render"
	"github.com/reugn/go-quartz/quartz"
	"gorm.io/gorm"
)

// JobRequest represents the request body for adding a job.
type JobRequest struct {
	Name     string `json:"name"`
	Group    string `json:"group"`
	Data     string `json:"data"`
	NextRun  int64  `json:"next_run"` // Unix timestamp
	Priority int    `json:"priority"`
}

// JobResponse represents the response for a job.
type JobResponse struct {
	ID       uint   `json:"id"`
	Name     string `json:"name"`
	Group    string `json:"group"`
	Data     string `json:"data"`
	NextRun  int64  `json:"next_run"`
	Priority int    `json:"priority"`
	Status   string `json:"status"`
}

// HTTPHandlers contains the handlers for the HTTP routes.
type HTTPHandlers struct {
	scheduler quartz.Scheduler
	db        *gorm.DB
}

// NewHTTPHandlers initializes the HTTP handlers.
func NewHTTPHandlers(scheduler quartz.Scheduler, db *gorm.DB) *HTTPHandlers {
	return &HTTPHandlers{scheduler: scheduler, db: db}
}

// GetUpcomingJobs returns a list of upcoming jobs.
func (h *HTTPHandlers) GetUpcomingJobs(w http.ResponseWriter, r *http.Request) {
	var jobs []jobs.ScheduledJob
	if err := h.db.Where("nxt_run_time > ?", time.Now().UnixNano()).Find(&jobs).Error; err != nil {
		render.Render(w, r, ErrInternalServerError(err))
		return
	}

	jobResponses := make([]JobResponse, 0, len(jobs))
	for _, job := range jobs {
		jobResponses = append(jobResponses, JobResponse{
			ID:      job.ID,
			Name:    job.JobName,
			Group:   job.JobGroup,
			Data:    job.JobData,
			NextRun: job.NextRunTime(),
			Status:  job.JobStatus,
		})
	}

	render.JSON(w, r, jobResponses)
}

// GetCompletedJobs returns a list of completed jobs filtered by timeframe and group name.
func (h *HTTPHandlers) GetCompletedJobs(w http.ResponseWriter, r *http.Request) {
	// Parse query parameters
	groupName := r.URL.Query().Get("group")
	startTime := r.URL.Query().Get("start_time")
	endTime := r.URL.Query().Get("end_time")

	// Parse timeframe
	var start, end time.Time
	if startTime != "" {
		start, _ = time.Parse(time.RFC3339, startTime)
	}
	if endTime != "" {
		end, _ = time.Parse(time.RFC3339, endTime)
	}

	// Build the query
	query := h.db.Where("job_status = ?", "completed")
	if groupName != "" {
		query = query.Where("job_group = ?", groupName)
	}
	if startTime != "" {
		query = query.Where("nxt_run_time >= ?", start.UnixNano())
	}
	if endTime != "" {
		query = query.Where("nxt_run_time <= ?", end.UnixNano())
	}

	// Fetch completed jobs
	var jobs []jobs.ScheduledJob
	if err := query.Find(&jobs).Error; err != nil {
		render.Render(w, r, ErrInternalServerError(err))
		return
	}

	jobResponses := make([]JobResponse, 0, len(jobs))
	for _, job := range jobs {
		jobResponses = append(jobResponses, JobResponse{
			ID:      job.ID,
			Name:    job.JobName,
			Group:   job.JobGroup,
			Data:    job.JobData,
			NextRun: job.NextRunTime(),
			Status:  job.JobStatus,
		})
	}

	render.JSON(w, r, jobResponses)
}

// AddJob adds a new job with a specified group name.
func (h *HTTPHandlers) AddJob(w http.ResponseWriter, r *http.Request) {
	var req JobRequest
	if err := render.DecodeJSON(r.Body, &req); err != nil {
		render.Render(w, r, ErrBadRequest(err))
		return
	}

	// Create a new job
	job := quartz.NewJobDetail(jobs.GetJob(req.Group), quartz.NewJobKeyWithGroup(req.Name, req.Group))
	trigger := quartz.NewRunOnceTrigger(time.Second * 1)

	// Schedule the job
	if err := h.scheduler.ScheduleJob(job, trigger); err != nil {
		render.Render(w, r, ErrInternalServerError(err))
		return
	}

	// Save the job to the database
	scheduledJob := jobs.ScheduledJob{
		JobKey:     job.JobKey().String(),
		JobName:    req.Name,
		JobGroup:   req.Group,
		JobData:    req.Data,
		NxtRunTime: req.NextRun,
		JobStatus:  "scheduled",
	}
	if err := h.db.Create(&scheduledJob).Error; err != nil {
		render.Render(w, r, ErrInternalServerError(err))
		return
	}

	render.JSON(w, r, JobResponse{
		ID:      scheduledJob.ID,
		Name:    scheduledJob.JobName,
		Group:   scheduledJob.JobGroup,
		Data:    scheduledJob.JobData,
		NextRun: scheduledJob.NextRunTime(),
		Status:  scheduledJob.JobStatus,
	})
}

// ErrResponse represents an error response.
type ErrResponse struct {
	Err            error `json:"-"`
	HTTPStatusCode int   `json:"-"`

	StatusText string `json:"status"`
	AppCode    int64  `json:"code,omitempty"`
	ErrorText  string `json:"error,omitempty"`
}

// Render implements the render.Renderer interface.
func (e *ErrResponse) Render(w http.ResponseWriter, r *http.Request) error {
	render.Status(r, e.HTTPStatusCode)
	return nil
}

// ErrBadRequest returns a 400 Bad Request error.
func ErrBadRequest(err error) render.Renderer {
	return &ErrResponse{
		Err:            err,
		HTTPStatusCode: http.StatusBadRequest,
		StatusText:     "Bad Request",
		ErrorText:      err.Error(),
	}
}

// ErrInternalServerError returns a 500 Internal Server Error.
func ErrInternalServerError(err error) render.Renderer {
	return &ErrResponse{
		Err:            err,
		HTTPStatusCode: http.StatusInternalServerError,
		StatusText:     "Internal Server Error",
		ErrorText:      err.Error(),
	}
}
