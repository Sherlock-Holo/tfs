package client

type fileHandle struct {
	file     *File
	writable bool
	readable bool
}
