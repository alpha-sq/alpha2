package api

import (
	"alpha2/crawler"
	"alpha2/jobs"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"time"

	"github.com/go-chi/chi/v5"
	"github.com/go-chi/chi/v5/middleware"
	"github.com/go-chi/cors"
	"github.com/go-chi/jwtauth/v5"
	"github.com/rs/zerolog/log"
	"github.com/spf13/viper"
)

var tokenAuth *jwtauth.JWTAuth

// var Secret string = viper.GetString("jwt.secret") // Replace <jwt-secret> with your secret key that is private to you.

func init() {
	tokenAuth = jwtauth.New("HS256", []byte("secet"), nil)
}

func init() {
	middleware.DefaultLogger = middleware.RequestLogger(&LogFormatter{})
}

func RunServer() {

	db := crawler.Conn()
	jobs.Init()

	if viper.GetBool("jobs.enable") {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		jobs.Scheduler.Start(ctx)
		log.Info().Msg("Jobs are enabled. Scheduler started.")
	} else {
		log.Info().Msg("Jobs are disabled. Set jobs.enable to true to enable job scheduling.")

	}

	// Create HTTP handlers
	handlers := NewHTTPHandlers(jobs.Scheduler, db)

	// Create a new Chi router
	r := chi.NewRouter()
	r.Use(middleware.Logger)
	r.Use(middleware.Recoverer)
	r.Use(cors.Handler(cors.Options{
		AllowedOrigins:   []string{viper.GetString("server.origins")}, // Allow your Next.js frontend
		AllowedMethods:   []string{"GET", "POST", "PUT", "PATCH", "DELETE", "OPTIONS"},
		AllowedHeaders:   []string{"Accept", "Authorization", "Content-Type", "X-CSRF-Token"},
		AllowCredentials: true,
		MaxAge:           300, // Maximum value not ignored by browsers
	}))

	// Register routes
	r.Group(func(r chi.Router) {
		r.Get("/jobs/upcoming", handlers.GetUpcomingJobs)
		r.Get("/jobs/completed", handlers.GetCompletedJobs)
		// r.Post("/jobs", handlers.AddJob)

		r.Get("/funds", getAllFunds)
		r.Get("/funds/explore", getExplorePMSData)
		r.Get("/fund/{fundID}/trailing-returns", getTrailingReturns)
		// r.Get("/funds/impact", getImpactData)
		// r.Get("/fund/{fundID}/rolling-returns", getRollingReturns)
		r.Get("/fund/{fundID}/discrete-returns", getDiscreteReturns)
		r.Get("/image", getImageHandler)
		r.Get("/fund-house/{slug}", getFundHouse)
		r.Get("/fund-house/aum/{slug}", getAUMChart)
	})

	r.Group(func(r chi.Router) {
		r.Post("/login", func(w http.ResponseWriter, r *http.Request) {
			r.ParseForm()
			userName := r.PostForm.Get("username")
			userPassword := r.PostForm.Get("password")

			if userName == "" || userPassword == "" {
				http.Error(w, "Missing username or password.", http.StatusBadRequest)
				return
			}
			if userName != viper.GetString("admin.username") || userPassword != viper.GetString("admin.password") {
				http.Error(w, "invalid username or password.", http.StatusBadRequest)
				return
			}

			token := MakeToken(userName)

			http.SetCookie(w, &http.Cookie{
				HttpOnly: true,
				Expires:  time.Now().Add(7 * 24 * time.Hour),
				SameSite: http.SameSiteLaxMode,
				Path:     "/",
				// Uncomment below for HTTPS:
				// Secure: true,
				Name:  "jwt", // Must be named "jwt" or else the token cannot be searched for by jwtauth.Verifier.
				Value: token,
			})

			body := make(map[string]string)
			body["token"] = token
			err := json.NewEncoder(w).Encode(body)
			if err != nil {
				http.Error(w, err.Error(), http.StatusInternalServerError)
				return
			}
		})

	})

	r.Group(func(r chi.Router) {
		r.Use(jwtauth.Verifier(tokenAuth))
		r.Use(jwtauth.Authenticator(tokenAuth))

		r.Get("/admin/fund-house", getFundHouseList)
		r.Get("/admin/fund-house/{ID}", getFundHouse)
		r.Patch("/admin/fund-house", updateFundHouse)

		r.Get("/admin/fund-house/{fund_house_id}/funds", getFundsListByFundHouse)
		r.Post("/admin/fund-house/{fund_house_id}/action/refetch-reports", reFetchReport)
		r.Post("/admin/fund-house/{fund_house_id}/fund/{fund_id}/action/hide", hideFund)
		r.Post("/admin/fund-house/{fund_house_id}/fund/{fund_id}/action/unhide", unhideFund)
		r.Post("/admin/fund-house/{fund_house_id}/fund/{fund_id}/action/unmerge", unmergeFund)
		r.Post("/admin/fund-house/{fund_house_id}/fund/{fund_id}/action/merge/{merge_fund_id}", mergeFund)

		r.Post("/upload", uploadHandler)
	})

	// Start the HTTP server
	log.Info().Str("port", viper.GetString("server.port")).Msg("Starting server")
	if err := http.ListenAndServe(fmt.Sprintf(":%s", viper.GetString("server.port")), r); err != nil {
		log.Fatal().Err(err).Msg("Failed to start server")
	}
}

func MakeToken(name string) string {
	_, tokenString, _ := tokenAuth.Encode(map[string]interface{}{"username": name})
	return tokenString
}
