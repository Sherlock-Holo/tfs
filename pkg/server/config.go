package server

import "time"

type Config struct {
	Root            string
	Address         string
	ShutdownTimeout time.Duration
}
