package main

import (
	"flag"
	"log"
	"os"

	"github.com/Sherlock-Holo/tfs/pkg/memfs"
)

func main() {
	mp := flag.String("m", "", "mount point")

	flag.Parse()

	if flag.NFlag() < 1 {
		flag.Usage()
		os.Exit(1)
	}

	if err := memfs.Run(*mp); err != nil {
		log.Fatal(err)
	}
}
