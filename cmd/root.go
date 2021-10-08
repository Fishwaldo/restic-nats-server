package cmd

import (
	"fmt"

	"github.com/Fishwaldo/restic-nats-server/cmd/rns"
	"github.com/Fishwaldo/restic-nats-server/internal/worker"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
)

var (
	// Used for flags.
	cfgFile     string

	rootCmd = &cobra.Command{
		Use:   "rns",
		Short: "Restic Nats Server",
		Long: `Restic Nats Server implements a worker based backend for Restic`,
		Run: func(cmd *cobra.Command, args []string) {
			LoadConfig()
			fmt.Println("Starting Services....")
			rns.StartServies()
			fmt.Println("Starting Workers....")
			worker.StartWorker()
		  },
	}
)

// Execute executes the root command.
func Execute() error {
	return rootCmd.Execute()
}

func init() {
	cobra.OnInitialize(initConfig)

	rootCmd.PersistentFlags().StringVarP(&cfgFile, "config", "c", "", "config file")
	rootCmd.PersistentFlags().Int("loglevel", 2, "Log Level To Run at")
	if (viper.IsSet("start-nats-server")) {
		rootCmd.PersistentFlags().Bool("start-nats-server", true, "Start Embedded Nats Server")
	}
	if (viper.IsSet("start-cache-server")) {
		rootCmd.PersistentFlags().Bool("start-cache-server", true, "Start Embedded Cache Server")
	}
	viper.BindPFlags(rootCmd.PersistentFlags())
}

func initConfig() {
	if cfgFile != "" {
		// Use config file from the flag.
		viper.SetConfigFile(cfgFile)
	} else {
		// Search config in home directory with name ".cobra" (without extension).
		viper.AddConfigPath(".")
		viper.AddConfigPath("conf")		
		viper.SetConfigType("json")
		viper.SetConfigName("rns")
	}

	viper.AutomaticEnv()

	if err := viper.ReadInConfig(); err == nil {
		fmt.Println("Using config file:", viper.ConfigFileUsed())
	} else {
		fmt.Printf("Error: %s\n", err)
		fmt.Println()
		fmt.Println()
		fmt.Println("===========================================================")
		fmt.Println("Config File Not Found. Running in Basic Embedded Demo Mode!")
		fmt.Println()
		fmt.Println("           This is only intended for testing!")
		fmt.Println()
		fmt.Println(" YOU MAY LOSE YOUR BACKUPS IF YOU RUN THIS AS PRODUCTION!")
		fmt.Println()
		fmt.Println("===========================================================")
		fmt.Println()
		fmt.Println()
	}
}
