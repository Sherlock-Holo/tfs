package main

import (
	"flag"
	"log"
	"os"
	"strings"

	"github.com/Sherlock-Holo/tfs/pkg/memfs"
)

func main() {
	mp := flag.String("m", "", "mount point")
	debug := flag.Bool("d", false, "debug mode")

	switch strings.ToLower(os.Getenv("DEBUG")) {
	case "1", "true":
		*debug = true
	}

	flag.Parse()

	if *mp == "" {
		flag.Usage()
		os.Exit(1)
	}

	if err := memfs.Run(*mp, *debug); err != nil {
		log.Fatal(err)
	}
}
