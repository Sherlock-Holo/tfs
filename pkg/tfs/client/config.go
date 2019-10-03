package client

import "time"

type Config struct {
	MountPoint      string
	TargetName      string
	Address         string
	InSecure        bool
	Debug           bool
	ShutdownTimeout time.Duration
}
