package internal

import (
	"os"
	"syscall"
	"time"

	"github.com/Sherlock-Holo/tfs/api/rpc"
	"github.com/golang/protobuf/ptypes"
	"github.com/hanwen/go-fuse/v2/fuse"
)

func CreateAttr(info os.FileInfo) *rpc.Attr {
	modifyTime, accessTime, changeTime := statTimes(info)

	mtime, statErr := ptypes.TimestampProto(modifyTime)
	if statErr != nil {
		mtime = ptypes.TimestampNow()
	}

	atime, statErr := ptypes.TimestampProto(accessTime)
	if statErr != nil {
		atime = ptypes.TimestampNow()
	}

	ctime, statErr := ptypes.TimestampProto(changeTime)
	if statErr != nil {
		ctime = ptypes.TimestampNow()
	}

	attr := &rpc.Attr{
		Name:       info.Name(),
		Mode:       int32(info.Mode()),
		Size:       info.Size(),
		ModifyTime: mtime,
		AccessTime: atime,
		ChangeTime: ctime,
	}

	if info.IsDir() {
		attr.Type = rpc.EntryType_dir
	} else {
		attr.Type = rpc.EntryType_file
	}

	return attr
}

func statTimes(info os.FileInfo) (modifyTime, accessTime, changeTime time.Time) {
	modifyTime = info.ModTime()

	stat, ok := info.Sys().(*syscall.Stat_t)
	if !ok {
		accessTime = modifyTime
		changeTime = modifyTime
		return
	}

	accessTime = time.Unix(stat.Atim.Sec, stat.Atim.Nsec)
	changeTime = time.Unix(stat.Ctim.Sec, stat.Ctim.Nsec)

	return
}

func SetEntryOutTime(atime, mtime, ctime time.Time, out *fuse.Attr) {
	out.Atime = uint64(atime.Unix())
	out.Atimensec = uint32(atime.UnixNano() - atime.Unix()*1_000_000_000)

	out.Mtime = uint64(mtime.Unix())
	out.Mtimensec = uint32(mtime.UnixNano() - mtime.Unix()*1_000_000_000)

	out.Ctime = uint64(ctime.Unix())
	out.Ctimensec = uint32(ctime.UnixNano() - ctime.Unix()*1_000_000_000)
}

func ChunkData(b []byte) [][]byte {
	if len(b) <= BufSize {
		return [][]byte{b}
	}

	head := b[:BufSize]
	return append([][]byte{head}, ChunkData(b[BufSize:])...)
}
