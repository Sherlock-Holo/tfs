package memfs

type fileHandle struct {
	file     *File
	writable bool
	readable bool
}
