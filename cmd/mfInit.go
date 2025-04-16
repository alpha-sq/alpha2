/*
Copyright © 2025 NAME HERE <EMAIL ADDRESS>
*/
package cmd

import (
	"alpha2/crawler/mf"
	"alpha2/jobs"
	"time"

	"github.com/reugn/go-quartz/quartz"
	"github.com/spf13/cobra"
)

// mfInitCmd represents the mfInit command
var mfInitCmd = &cobra.Command{
	Use:   "mfInit",
	Short: "A brief description of your command",
	Long: `A longer description that spans multiple lines and likely contains examples
and usage of using your command. For example:

Cobra is a CLI library for Go that empowers applications.
This application is a tool to generate the needed files
to quickly create a Cobra application.`,
	Run: func(cmd *cobra.Command, args []string) {
		jobs.Init()

		jd := quartz.NewJobDetailWithOptions(&mf.MFSync{}, quartz.NewJobKeyWithGroup("MFSync", "MFSync"), &quartz.JobDetailOptions{
			MaxRetries:    10,
			RetryInterval: time.Minute * 5,
			Replace:       false,
			Suspended:     false,
		})
		t := quartz.NewRunOnceTrigger(time.Second * 5)

		jobs.Scheduler.ScheduleJob(jd, t)
	},
}

func init() {
	rootCmd.AddCommand(mfInitCmd)

}
