package server

import "os"

type file struct {
	f     *os.File
	count int32
}
