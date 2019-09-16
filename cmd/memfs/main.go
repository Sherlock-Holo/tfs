package main

import (
	"flag"
	"log"
	"os"

	"github.com/Sherlock-Holo/tfs/pkg/memfs"
)

func main() {
	mp := flag.String("m", "", "mount point")
	debug := flag.Bool("d", false, "debug mode")

	flag.Parse()

	if *mp == "" {
		flag.Usage()
		os.Exit(1)
	}

	if err := memfs.Run(*mp, *debug); err != nil {
		log.Fatal(err)
	}
}
