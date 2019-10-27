package fs

import "time"

const (
	fileTimeout          = time.Minute
	fileAllocateChanSize = 1
	fileReadChanSize     = 1
	fileWriteChanSize    = 1

	// fsAttrChanSize = 10
)
