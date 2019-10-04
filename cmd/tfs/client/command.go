package main

import (
	"github.com/Sherlock-Holo/tfs/cmd/tfs"
	"github.com/Sherlock-Holo/tfs/pkg/tfs/client"
	log "github.com/sirupsen/logrus"
	"github.com/spf13/cobra"
)

var (
	rootCmd = &cobra.Command{
		Use:     "tfsc <mount-point> <tfs-server>",
		Short:   "tfsc is a client of tfs fuse",
		Version: tfs.Version,
		Args:    cobra.ExactArgs(2),
		RunE: func(_ *cobra.Command, args []string) error {
			if verbose {
				log.SetLevel(log.DebugLevel)
			}

			cfg := client.Config{
				Debug:      debug,
				MountPoint: args[0],
				Address:    args[1],
				InSecure:   inSecure,
			}

			return client.Run(cfg)
		},
	}

	verbose  bool
	debug    bool
	inSecure bool
)

func execute() {
	rootCmd.Flags().BoolVarP(&verbose, "verbose", "V", false, "verbose log")
	rootCmd.Flags().BoolVarP(&debug, "debug", "d", false, "debug fuse log")
	rootCmd.Flags().BoolVarP(&inSecure, "insecure", "", false, "disable server authentication")

	rootCmd.InitDefaultVersionFlag()

	if err := rootCmd.Execute(); err != nil {
		log.Fatalf("%+v", err)
	}
}
