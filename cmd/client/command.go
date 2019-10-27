package main

import (
	"time"

	"github.com/Sherlock-Holo/tfs/cmd"
	"github.com/Sherlock-Holo/tfs/pkg/client"
	log "github.com/sirupsen/logrus"
	"github.com/spf13/cobra"
)

var (
	rootCmd = &cobra.Command{
		Use:     "tfsc <mount-point> <tfs-server>",
		Short:   "tfsc is a client of tfs fuse",
		Version: cmd.Version,
		Args:    cobra.ExactArgs(2),
		RunE: func(_ *cobra.Command, args []string) error {
			if verbose {
				log.SetLevel(log.DebugLevel)
			}

			cfg := client.Config{
				Debug:       debug,
				MountPoint:  args[0],
				Address:     args[1],
				InSecure:    inSecure,
				CallTimeout: callTimeout,
			}

			return client.Run(cfg)
		},
	}

	verbose     bool
	debug       bool
	inSecure    bool
	callTimeout time.Duration
)

func execute() {
	rootCmd.Flags().BoolVarP(&verbose, "verbose", "V", false, "verbose log")
	rootCmd.Flags().BoolVarP(&debug, "debug", "d", false, "debug fuse log")
	rootCmd.Flags().BoolVarP(&inSecure, "insecure", "", false, "disable server authentication")
	rootCmd.Flags().DurationVarP(&callTimeout, "timeout", "", 10*time.Second, "rpc call timeout")

	rootCmd.InitDefaultVersionFlag()

	if err := rootCmd.Execute(); err != nil {
		log.Fatalf("%+v", err)
	}
}
