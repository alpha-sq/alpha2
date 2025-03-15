package api

import (
	"alpha2/crawler"
	"alpha2/jobs"
	"context"
	"log"
	"net/http"

	"github.com/go-chi/chi/v5"
	"github.com/go-chi/chi/v5/middleware"
)

func init() {
	middleware.DefaultLogger = middleware.RequestLogger(&LogFormatter{})
}

func RunServer() {

	db := crawler.Conn()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	jobs.Scheduler.Start(ctx)

	// Create HTTP handlers
	handlers := NewHTTPHandlers(jobs.Scheduler, db)

	// Create a new Chi router
	r := chi.NewRouter()
	r.Use(middleware.Logger)
	r.Use(middleware.Recoverer)

	// Register routes
	r.Group(func(r chi.Router) {
		r.Use(middleware.AllowContentType("application/json"))

		r.Get("/jobs/upcoming", handlers.GetUpcomingJobs)
		r.Get("/jobs/completed", handlers.GetCompletedJobs)
		r.Post("/jobs", handlers.AddJob)
	})

	// Start the HTTP server
	log.Println("Starting server on :8080")
	if err := http.ListenAndServe(":8080", r); err != nil {
		log.Fatalf("Failed to start server: %v", err)
	}
}
