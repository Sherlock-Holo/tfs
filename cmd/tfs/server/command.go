package main

import (
	"github.com/Sherlock-Holo/tfs/cmd/tfs"
	"github.com/Sherlock-Holo/tfs/pkg/tfs/server"
	log "github.com/sirupsen/logrus"
	"github.com/spf13/cobra"
)

var (
	rootCmd = &cobra.Command{
		Use:     "tfsd <root> <listen-address>",
		Short:   "tfsd is a daemon of tfs fuse",
		Version: tfs.Version,
		Args:    cobra.ExactArgs(2),
		RunE: func(_ *cobra.Command, args []string) error {
			if verbose {
				log.SetLevel(log.DebugLevel)
			}

			cfg := server.Config{
				Root:    args[0],
				Address: args[1],
			}

			return server.Run(cfg)
		},
	}

	verbose bool
)

func execute() {
	rootCmd.Flags().BoolVarP(&verbose, "verbose", "V", false, "verbose log")

	rootCmd.InitDefaultVersionFlag()

	if err := rootCmd.Execute(); err != nil {
		log.Fatalf("%+v", err)
	}
}
