package memfs

type fileHandle struct {
	file     *File
	offset   uint64
	writable bool
	readable bool
}
