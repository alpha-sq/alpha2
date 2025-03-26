/*
Copyright © 2025 NAME HERE <EMAIL ADDRESS>
*/
package cmd

import (
	"alpha2/crawler/pmf"
	"alpha2/jobs"
	"context"
	"time"

	"github.com/reugn/go-quartz/quartz"
	"github.com/spf13/cobra"
)

// pmfInitCmd represents the pmfInit command
var pmfInitCmd = &cobra.Command{
	Use:   "pmfInit",
	Short: "A brief description of your command",
	Long: `A longer description that spans multiple lines and likely contains examples
and usage of using your command. For example:

Cobra is a CLI library for Go that empowers applications.
This application is a tool to generate the needed files
to quickly create a Cobra application.`,
	Run: func(cmd *cobra.Command, args []string) {
		jobs.Init()
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		jobs.Scheduler.Start(ctx)

		jd := quartz.NewJobDetailWithOptions(&pmf.PMFInit{}, quartz.NewJobKeyWithGroup("PMFInit", "PMFInit"), &quartz.JobDetailOptions{
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
	rootCmd.AddCommand(pmfInitCmd)

	// Here you will define your flags and configuration settings.

	// Cobra supports Persistent Flags which will work for this command
	// and all subcommands, e.g.:
	// pmfInitCmd.PersistentFlags().String("foo", "", "A help for foo")

	// Cobra supports local flags which will only run when this command
	// is called directly, e.g.:
	// pmfInitCmd.Flags().BoolP("toggle", "t", false, "Help message for toggle")
}
