package memfs

import (
	"time"

	"github.com/hanwen/go-fuse/v2/fuse"
)

func setEntryOutTime(atime, mtime, ctime time.Time, out *fuse.Attr) {
	out.Atime = uint64(atime.Unix())
	out.Atimensec = uint32(atime.UnixNano() - atime.Unix()*1_000_000_000)

	out.Mtime = uint64(mtime.Unix())
	out.Mtimensec = uint32(mtime.UnixNano() - mtime.Unix()*1_000_000_000)

	out.Ctime = uint64(ctime.Unix())
	out.Ctimensec = uint32(ctime.UnixNano() - ctime.Unix()*1_000_000_000)
}
