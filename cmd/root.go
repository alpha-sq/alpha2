/*
Copyright © 2025 NAME HERE <EMAIL ADDRESS>
*/
package cmd

import (
	"alpha2/crawler"
	"alpha2/crawler/pmf"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"strings"
	"time"

	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
	"gorm.io/gorm/clause"
)

// rootCmd represents the base command when called without any subcommands
var rootCmd = &cobra.Command{
	Use:   "alpha2",
	Short: "Web scraping tool for mutual fund data",
	Long:  `Alpha2 is a web scraping tool for mutual fund data. It scrapes data from the AMFI website and stores it in an Database file.`,
	PreRunE: func(cmd *cobra.Command, args []string) error {
		// if err := cmd.MarkFlagRequired("date"); err != nil {
		// 	return err
		// }
		if err := cmd.MarkFlagFilename("log-file"); err != nil {
			return err
		}
		return nil
	},
	Run: func(cmd *cobra.Command, args []string) {
		if fromDate != "" && toDate != "" {
			fromDate_, _ := time.Parse("2006-01-02", fromDate)
			toDate_, _ := time.Parse("2006-01-02", toDate)
			for forDate := fromDate_; forDate.After(toDate_); forDate = forDate.AddDate(0, -1, 0) {
				runClawl(&forDate)
			}
		} else {
			forDate, _ := time.Parse("2006-01-02", targetDate)
			runClawl(&forDate)
		}
	},
}

// Execute adds all child commands to the root command and sets flags appropriately.
// This is called by main.main(). It only needs to happen once to the rootCmd.
func Execute() {
	err := rootCmd.Execute()
	if err != nil {
		os.Exit(1)
	}
}

var targetDate string
var fromDate string
var toDate string
var cfgFile string

func init() {
	// Here you will define your flags and configuration settings.
	// Cobra supports persistent flags, which, if defined here,
	// will be global for your application.
	cobra.OnInitialize(initConfig)
	cobra.OnInitialize(setuplogger)

	rootCmd.PersistentFlags().StringVar(&cfgFile, "config", "", "config file (default is $HOME/.alpha2.yaml)")
	rootCmd.PersistentFlags().StringVarP(&targetDate, "date", "d", "", "Date for which the data is to be scraped, in the format YYYY-MM-DD")
	rootCmd.PersistentFlags().StringVar(&fromDate, "from", "", "In the format YYYY-MM-DD")
	rootCmd.PersistentFlags().StringVar(&toDate, "to", "", "In the format YYYY-MM-DD")
	viper.BindPFlag("date", rootCmd.PersistentFlags().Lookup("date"))

	rootCmd.PersistentFlags().String("log-level", "info", "Set log level (debug, info, warn, error)")
	viper.BindPFlag("log.level", rootCmd.PersistentFlags().Lookup("log-level"))

	rootCmd.PersistentFlags().String("log-out", "stdout", "Set log file (leave empty for stdout)")
	// viper.BindPFlag("log.out", rootCmd.PersistentFlags().Lookup("log-file"))

	rootCmd.PersistentFlags().String("log-file", "app.log", "Set log file (leave empty for stdout)")
	viper.BindPFlag("log.file", rootCmd.PersistentFlags().Lookup("log-file"))
}

func setuplogger() {
	// Set up logger
	zerolog.TimeFieldFormat = zerolog.TimeFormatUnix

	var logWriter io.Writer
	logOut := viper.GetString("log.out")
	switch logOut {
	case "stdout":
		logWriter = zerolog.ConsoleWriter{Out: os.Stdout, NoColor: false, TimeFormat: time.RFC822}
	case "observe":
		logWriter = OpenObserveWriter{}
	default:
		logFile := viper.GetString("log.file")
		file, err := os.OpenFile(logFile, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0666)
		if err != nil {
			log.Fatal().Err(err).Str("log-file", logFile).Msg("Error opening log file")
		}
		logWriter = zerolog.ConsoleWriter{Out: file, NoColor: true, TimeFormat: time.RFC1123}
	}

	level, err := zerolog.ParseLevel(strings.ToLower(viper.GetString("log.level")))
	if err != nil {
		level = zerolog.ErrorLevel
	}

	zerolog.TimestampFieldName = "timestamp"
	log.Logger = zerolog.New(logWriter).Level(level).With().Timestamp().Logger()

}

type OpenObserveWriter struct{}

func (w OpenObserveWriter) Write(data []byte) (n int, err error) {
	req, _ := http.NewRequest("POST", viper.GetString("openobserve.url"), strings.NewReader(string(data)))
	req.SetBasicAuth(viper.GetString("openobserve.user"), viper.GetString("openobserve.password"))
	req.Header.Set("Content-Type", "application/json")

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return 0, err
	}
	defer resp.Body.Close()

	return len(data), nil
}

func initConfig() {
	if cfgFile != "" {
		// Use config file from the flag.
		viper.SetConfigFile(cfgFile)
	} else {
		// Find home directory.
		home, err := os.UserHomeDir()
		cobra.CheckErr(err)

		// Search config in home directory with name ".cobra" (without extension).
		viper.AddConfigPath(home)
		viper.SetConfigType("yaml")
		viper.SetConfigName(".alpha2")
	}

	viper.AutomaticEnv()

	if err := viper.ReadInConfig(); err != nil {
		log.Fatal().Err(err).Msg("Error reading config file")
	}
	fmt.Println("Using config file:", viper.ConfigFileUsed())
}

func runClawl(forDate *time.Time) {
	craw := pmf.NewPMFCrawler()
	db := crawler.Conn()
	craw.CrawlAllFund(forDate, func(funds []*crawler.Fund) {
		for _, fund := range funds {
			err := db.Model(&crawler.Fund{}).Clauses(clause.OnConflict{
				DoNothing: true,
			}).Create(fund)
			if err.Error != nil {
				jsonfund, _ := json.Marshal(fund)
				log.Error().Err(err.Error).RawJSON("fund", jsonfund).Msg("Error while saving funds")
			}
		}
	})
}
