package api

import (
	"alpha2/crawler"
	"alpha2/jobs"
	"context"
	"fmt"
	"net/http"

	"github.com/go-chi/chi/v5"
	"github.com/go-chi/chi/v5/middleware"
	"github.com/go-chi/cors"
	"github.com/rs/zerolog/log"
	"github.com/spf13/viper"
)

func init() {
	middleware.DefaultLogger = middleware.RequestLogger(&LogFormatter{})
}

func RunServer() {

	db := crawler.Conn()
	jobs.Init()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	if viper.GetBool("jobs.enable") {
		jobs.Scheduler.Start(ctx)
	}

	// Create HTTP handlers
	handlers := NewHTTPHandlers(jobs.Scheduler, db)

	// Create a new Chi router
	r := chi.NewRouter()
	r.Use(middleware.Logger)
	r.Use(middleware.Recoverer)
	r.Use(cors.Handler(cors.Options{
		AllowedOrigins:   []string{"*"}, // Allow your Next.js frontend
		AllowedMethods:   []string{"GET", "POST", "PUT", "DELETE", "OPTIONS"},
		AllowedHeaders:   []string{"Accept", "Authorization", "Content-Type", "X-CSRF-Token"},
		AllowCredentials: true,
		MaxAge:           300, // Maximum value not ignored by browsers
	}))

	// Register routes
	r.Group(func(r chi.Router) {
		r.Get("/jobs/upcoming", handlers.GetUpcomingJobs)
		r.Get("/jobs/completed", handlers.GetCompletedJobs)
		r.Post("/jobs", handlers.AddJob)

		r.Get("/funds", getAllFunds)
		r.Get("/funds/explore", getExplorePMSData)
		r.Get("/fund/{fundID}/trailing-returns", getTrailingReturns)
		// r.Get("/funds/impact", getImpactData)
		// r.Get("/fund/{fundID}/rolling-returns", getRollingReturns)
		r.Get("/fund/{fundID}/discrete-returns", getDiscreteReturns)
	})

	// Start the HTTP server
	log.Info().Str("port", viper.GetString("server.port")).Msg("Starting server")
	if err := http.ListenAndServe(fmt.Sprintf(":%s", viper.GetString("server.port")), r); err != nil {
		log.Fatal().Err(err).Msg("Failed to start server")
	}
}
