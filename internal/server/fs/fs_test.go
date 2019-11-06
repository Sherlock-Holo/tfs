package fs

import (
	"bytes"
	"context"
	"io/ioutil"
	"math/rand"
	"os"
	"path/filepath"
	"reflect"
	"testing"
	"time"

	"github.com/Sherlock-Holo/errors"
	"github.com/Sherlock-Holo/tfs/api/rpc"
	"github.com/Sherlock-Holo/tfs/internal"
)

func TestTree_doLookupReq(t *testing.T) {
	tests := []struct {
		name     string
		testFunc func(tt *testing.T)
	}{
		{
			"lookup success",
			testTreeLookupSuccess,
		},
		{
			"lookup not found",
			testTreeLookupNotFound,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, tt.testFunc)
	}
}

func testTreeLookupSuccess(t *testing.T) {
	root := os.TempDir()
	tmpFile, err := ioutil.TempFile(root, "")
	if err != nil {
		t.Fatal(err)
	}
	name := tmpFile.Name()
	defer func() {
		_ = tmpFile.Close()
		_ = os.Remove(name)
	}()

	tree := NewTree(root)
	tree.Run()
	defer tree.Shutdown()

	info, err := tmpFile.Stat()
	if err != nil {
		t.Fatal(err)
	}

	respCh := make(chan LookupResponse, 1)
	req := LookupRequest{
		Ctx:     context.Background(),
		DirPath: root,
		Name:    filepath.Base(name),
		RespCh:  respCh,
	}

	select {
	default:
		t.Fatal("send lookup request to fs tree failed, exit test")

	case tree.lookupCh <- req:
	}

	gotResp := <-respCh
	if gotResp.Err != nil {
		t.Fatal(err)
	}

	expectAttr := internal.CreateAttr(info)

	if !reflect.DeepEqual(gotResp.Attr, expectAttr) {
		t.Fatalf("attr not equal, got %v, expect %v", gotResp.Attr, expectAttr)
	}
}

func testTreeLookupNotFound(t *testing.T) {
	root := os.TempDir()

	tree := NewTree(root)
	tree.Run()
	defer tree.Shutdown()

	respCh := make(chan LookupResponse, 1)
	req := LookupRequest{
		Ctx:     context.Background(),
		DirPath: root,
		Name:    filepath.Base("lookup-not-found"),
		RespCh:  respCh,
	}

	select {
	default:
		t.Fatal("send lookup request to fs tree failed, exit test")

	case tree.lookupCh <- req:
	}

	gotResp := <-respCh
	if gotResp.Attr != nil {
		t.Fatalf("expect got no attr, but got one attr %v", gotResp.Attr)
	}

	if !errors.Is(gotResp.Err, os.ErrNotExist) {
		t.Fatalf("got error %v, expect got error %v", gotResp.Err, os.ErrNotExist)
	}
}

func TestTree_doAttrReq(t *testing.T) {
	tests := []struct {
		name     string
		testFunc func(tt *testing.T)
	}{
		{
			"get attr",
			testTreeGetAttrReq,
		},
		{
			"set attr",
			testTreeSetAttrReq,
		},
		{
			"get attr not found",
			testTreeGetAttrReqNotFound,
		},
		{
			"set attr not found",
			testTreeSetAttrReqNotFound,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, tt.testFunc)
	}
}

func testTreeGetAttrReq(t *testing.T) {
	root := os.TempDir()
	tmpFile, err := ioutil.TempFile(root, "")
	if err != nil {
		t.Fatal(err)
	}
	name := tmpFile.Name()
	defer func() {
		_ = tmpFile.Close()
		_ = os.Remove(name)
	}()

	info, err := tmpFile.Stat()
	if err != nil {
		t.Fatal(err)
	}

	tree := NewTree(root)
	tree.Run()
	defer tree.Shutdown()

	respCh := make(chan AttrResponse, 1)

	req := AttrRequest{
		Ctx:    context.Background(),
		Name:   filepath.Base(name),
		Op:     AttrGet,
		RespCh: respCh,
	}

	select {
	default:
		t.Fatal("send get attr request to fs tree failed, exit test")

	case tree.attrCh <- req:
	}

	gotResp := <-respCh
	if gotResp.Err != nil {
		t.Fatal(err)
	}

	expectAttr := internal.CreateAttr(info)

	if !reflect.DeepEqual(gotResp.Attr, expectAttr) {
		t.Fatalf("attr not equal, got %v, expect %v", gotResp.Attr, expectAttr)
	}
}

func testTreeSetAttrReq(t *testing.T) {
	root := os.TempDir()
	tmpFile, err := ioutil.TempFile(root, "")
	if err != nil {
		t.Fatal(err)
	}
	name := tmpFile.Name()
	defer func() {
		_ = tmpFile.Close()
		_ = os.Remove(name)
	}()

	data := make([]byte, 100)
	rand.Seed(time.Now().UnixNano())
	rand.Read(data)
	if _, err := tmpFile.Write(data); err != nil {
		t.Fatal(err)
	}

	tree := NewTree(root)
	tree.Run()
	defer tree.Shutdown()

	respCh := make(chan AttrResponse, 1)

	req := AttrRequest{
		Ctx:  context.Background(),
		Name: filepath.Base(name),
		Op:   AttrSet,
		Attr: &rpc.Attr{
			Name: filepath.Base(name),
			Mode: 0600,
			Size: 10,
		},
		RespCh: respCh,
	}

	select {
	default:
		t.Fatal("send set attr request to fs tree failed, exit test")

	case tree.attrCh <- req:
	}

	gotResp := <-respCh

	info, err := tmpFile.Stat()
	if err != nil {
		t.Fatal("get tmp file info failed:", errors.WithStack(err))
	}

	if gotResp.Err != nil {
		t.Fatal(err)
	}

	expectAttr := internal.CreateAttr(info)

	if !reflect.DeepEqual(gotResp.Attr, expectAttr) {
		t.Fatalf("attr not equal, got %v, expect %v", gotResp.Attr, expectAttr)
	}
}

func testTreeGetAttrReqNotFound(t *testing.T) {
	root := os.TempDir()
	tree := NewTree(root)
	tree.Run()
	defer tree.Shutdown()

	respCh := make(chan AttrResponse, 1)

	req := AttrRequest{
		Ctx:    context.Background(),
		Name:   filepath.Base("not found"),
		Op:     AttrGet,
		RespCh: respCh,
	}

	select {
	default:
		t.Fatal("send get attr request to fs tree failed, exit test")

	case tree.attrCh <- req:
	}

	gotResp := <-respCh
	if gotResp.Attr != nil {
		t.Fatalf("expect got no attr, but got one %v", gotResp.Attr)
	}

	if !errors.Is(gotResp.Err, os.ErrNotExist) {
		t.Fatalf("expect error %v, got error %v", gotResp.Err, os.ErrNotExist)
	}
}

func testTreeSetAttrReqNotFound(t *testing.T) {
	root := os.TempDir()
	tree := NewTree(root)
	tree.Run()
	defer tree.Shutdown()

	respCh := make(chan AttrResponse, 1)

	req := AttrRequest{
		Ctx:  context.Background(),
		Name: filepath.Base("not found"),
		Op:   AttrSet,
		Attr: &rpc.Attr{
			Mode: 0644,
			Size: 10,
		},
		RespCh: respCh,
	}

	select {
	default:
		t.Fatal("send set attr request to fs tree failed, exit test")

	case tree.attrCh <- req:
	}

	gotResp := <-respCh
	if gotResp.Attr != nil {
		t.Fatalf("expect got no attr, but got one %v", gotResp.Attr)
	}

	if !errors.Is(gotResp.Err, os.ErrNotExist) {
		t.Fatalf("expect error %v, got error %v", gotResp.Err, os.ErrNotExist)
	}
}

func TestTree_doCreateReq(t *testing.T) {
	tests := []struct {
		name     string
		testFunc func(t *testing.T)
	}{
		{
			"create file success",
			testTreeDoCreateReqSuccess,
		},
		{
			"create file dir not found",
			testTreeDoCreateReqDirNotFound,
		},
		{
			"create file exist",
			testTreeDoCreateReqFileExist,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, tt.testFunc)
	}
}

func testTreeDoCreateReqSuccess(t *testing.T) {
	root := os.TempDir()

	name := "test-file"
	defer func() {
		_ = os.Remove(filepath.Join(root, name))
	}()

	tree := NewTree(root)
	tree.Run()
	defer tree.Shutdown()

	respCh := make(chan CreateResponse, 1)
	req := CreateRequest{
		Ctx:     context.Background(),
		DirPath: "/",
		Name:    name,
		Mode:    0600,
		RespCh:  respCh,
	}

	select {
	default:
		t.Fatal("send create file request to fs tree failed, exit test")

	case tree.createCh <- req:
	}

	gotResp := <-respCh

	info, err := os.Stat(filepath.Join(root, name))
	if err != nil {
		t.Fatal("get tmp file info failed:", errors.WithStack(err))
	}

	if gotResp.Err != nil {
		t.Fatal(err)
	}

	expectAttr := internal.CreateAttr(info)

	if !reflect.DeepEqual(gotResp.Attr, expectAttr) {
		t.Fatalf("attr not equal, got %v, expect %v", gotResp.Attr, expectAttr)
	}
}

func testTreeDoCreateReqDirNotFound(t *testing.T) {
	root := os.TempDir()
	name := "test-file"

	tree := NewTree(root)
	tree.Run()
	defer tree.Shutdown()

	respCh := make(chan CreateResponse, 1)
	req := CreateRequest{
		Ctx:     context.Background(),
		DirPath: "/not-found",
		Name:    name,
		Mode:    0600,
		RespCh:  respCh,
	}

	select {
	default:
		t.Fatal("send create file request to fs tree failed, exit test")

	case tree.createCh <- req:
	}

	gotResp := <-respCh

	if gotResp.Attr != nil {
		t.Fatalf("expect got no attr, but got one %v", gotResp.Attr)
	}

	if !errors.Is(gotResp.Err, os.ErrNotExist) {
		t.Fatalf("got error %v, expect got error %v", gotResp.Err, os.ErrNotExist)
	}
}

func testTreeDoCreateReqFileExist(t *testing.T) {
	root := os.TempDir()

	tmpFile, err := ioutil.TempFile(root, "")
	if err != nil {
		t.Fatal(err)
	}

	defer func() {
		_ = tmpFile.Close()
		_ = os.Remove(tmpFile.Name())
	}()

	tree := NewTree(root)
	tree.Run()
	defer tree.Shutdown()

	respCh := make(chan CreateResponse, 1)
	req := CreateRequest{
		Ctx:     context.Background(),
		DirPath: "/",
		Name:    filepath.Base(tmpFile.Name()),
		Mode:    0600,
		RespCh:  respCh,
	}

	select {
	default:
		t.Fatal("send create file request to fs tree failed, exit test")

	case tree.createCh <- req:
	}

	gotResp := <-respCh

	if gotResp.Attr != nil {
		t.Fatalf("expect got no attr, but got one %v", gotResp.Attr)
	}

	if !errors.Is(gotResp.Err, os.ErrExist) {
		t.Fatalf("got error %v, expect got error %v", gotResp.Err, os.ErrNotExist)
	}
}

func TestTree_doMkdirReq(t *testing.T) {
	tests := []struct {
		name     string
		testFunc func(t *testing.T)
	}{
		{
			"mkdir success",
			testTreeDoMkdirReqSuccess,
		},
		{
			"mkdir exist",
			testTreeDoMkdirReqExist,
		},
		{
			"mkdir parent not exist",
			testTreeDoMkdirReqParentNotExist,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, tt.testFunc)
	}
}

func testTreeDoMkdirReqSuccess(t *testing.T) {
	root := os.TempDir()

	name := "test-dir"
	defer func() {
		_ = os.Remove(filepath.Join(root, name))
	}()

	tree := NewTree(root)
	tree.Run()
	defer tree.Shutdown()

	respCh := make(chan MkdirResponse, 1)
	req := MkdirRequest{
		Ctx:     context.Background(),
		DirPath: "/",
		Name:    name,
		RespCh:  respCh,
	}

	select {
	default:
		t.Fatal("send mkdir request to fs tree failed, exit test")

	case tree.mkdirCh <- req:
	}

	gotResp := <-respCh

	info, err := os.Stat(filepath.Join(root, name))
	if err != nil {
		t.Fatal("get tmp mkdir info failed:", errors.WithStack(err))
	}

	if gotResp.Err != nil {
		t.Fatal(err)
	}

	expectAttr := internal.CreateAttr(info)
	if !reflect.DeepEqual(gotResp.Attr, expectAttr) {
		t.Fatalf("attr not equal, got %v, expect %v", gotResp.Attr, expectAttr)
	}
}

func testTreeDoMkdirReqExist(t *testing.T) {
	root := os.TempDir()

	name, err := ioutil.TempDir(root, "")
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		_ = os.Remove(filepath.Join(root, name))
	}()

	tree := NewTree(root)
	tree.Run()
	defer tree.Shutdown()

	respCh := make(chan MkdirResponse, 1)
	req := MkdirRequest{
		Ctx:     context.Background(),
		DirPath: "/",
		Name:    filepath.Base(name),
		RespCh:  respCh,
	}

	select {
	default:
		t.Fatal("send mkdir request to fs tree failed, exit test")

	case tree.mkdirCh <- req:
	}

	gotResp := <-respCh

	if gotResp.Attr != nil {
		t.Fatalf("expect got no attr, but got one attr %v", gotResp.Attr)
	}

	if !errors.Is(gotResp.Err, os.ErrExist) {
		t.Fatalf("got error %v, expect got error %v", gotResp.Err, os.ErrNotExist)
	}
}

func testTreeDoMkdirReqParentNotExist(t *testing.T) {
	root := os.TempDir()

	name := "test-dir"
	if err := os.Mkdir(filepath.Join(root, name), os.ModeDir|0600); err != nil {
		t.Fatalf("%v", errors.WithStack(err))
	}
	defer func() {
		_ = os.Remove(filepath.Join(root, name))
	}()

	tree := NewTree(root)
	tree.Run()
	defer tree.Shutdown()

	respCh := make(chan MkdirResponse, 1)
	req := MkdirRequest{
		Ctx:     context.Background(),
		DirPath: "/not-exist",
		Name:    name,
		RespCh:  respCh,
	}

	select {
	default:
		t.Fatal("send mkdir request to fs tree failed, exit test")

	case tree.mkdirCh <- req:
	}

	gotResp := <-respCh

	if gotResp.Attr != nil {
		t.Fatalf("expect got no attr, but got one attr %v", gotResp.Attr)
	}

	if !errors.Is(gotResp.Err, os.ErrNotExist) {
		t.Fatalf("got error %v, expect got error %v", gotResp.Err, os.ErrNotExist)
	}
}

func TestTree_doAllocateReq(t *testing.T) {
	tests := []struct {
		name     string
		testFunc func(t *testing.T)
	}{
		{
			"allocate success with offset 0 size 100",
			testTreeDoAllocateReqOffset0Success,
		},
		{
			"allocate success with offset 10 size 100",
			testTreeDoAllocateReqOffset10Success,
		},
		{
			"allocate file not exist",
			testTreeDoAllocateReqNotExist,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, tt.testFunc)
	}
}

func testTreeDoAllocateReqOffset0Success(t *testing.T) {
	root := os.TempDir()

	file, err := ioutil.TempFile(root, "")
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		name := file.Name()
		_ = file.Close()
		_ = os.Remove(name)
	}()

	tree := NewTree(root)
	tree.Run()
	defer tree.Shutdown()

	respCh := make(chan error, 1)
	req := AllocateRequest{
		Ctx:    context.Background(),
		Name:   filepath.Join("/", filepath.Base(file.Name())),
		Offset: 0,
		Size:   100,
		RespCh: respCh,
	}

	select {
	default:
		t.Fatal("send create file request to fs tree failed, exit test")

	case tree.allocateCh <- req:
	}

	gotResp := <-respCh

	info, err := file.Stat()
	if err != nil {
		t.Fatal(err)
	}

	if gotResp != nil {
		t.Fatal(err)
	}

	attr := internal.CreateAttr(info)

	if uint64(attr.Size) != req.Offset+req.Size {
		t.Fatalf("got size %d, expect %d", attr.Size, req.Offset+req.Size)
	}
}

func testTreeDoAllocateReqOffset10Success(t *testing.T) {
	root := os.TempDir()

	file, err := ioutil.TempFile(root, "")
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		name := file.Name()
		_ = file.Close()
		_ = os.Remove(name)
	}()

	tree := NewTree(root)
	tree.Run()
	defer tree.Shutdown()

	respCh := make(chan error, 1)
	req := AllocateRequest{
		Ctx:    context.Background(),
		Name:   filepath.Join("/", filepath.Base(file.Name())),
		Offset: 10,
		Size:   100,
		RespCh: respCh,
	}

	select {
	default:
		t.Fatal("send create file request to fs tree failed, exit test")

	case tree.allocateCh <- req:
	}

	gotResp := <-respCh

	info, err := file.Stat()
	if err != nil {
		t.Fatal(err)
	}

	if gotResp != nil {
		t.Fatal(err)
	}

	attr := internal.CreateAttr(info)

	if uint64(attr.Size) != req.Offset+req.Size {
		t.Fatalf("got size %d, expect %d", attr.Size, req.Offset+req.Size)
	}
}

func testTreeDoAllocateReqNotExist(t *testing.T) {
	root := os.TempDir()

	name := "allocate-test-file-not-exist"

	tree := NewTree(root)
	tree.Run()
	defer tree.Shutdown()

	respCh := make(chan error, 1)
	req := AllocateRequest{
		Ctx:    context.Background(),
		Name:   filepath.Join("/", "not-found", name),
		Offset: 0,
		Size:   100,
		RespCh: respCh,
	}

	select {
	default:
		t.Fatal("send create file request to fs tree failed, exit test")

	case tree.allocateCh <- req:
	}

	gotResp := <-respCh

	if !errors.Is(gotResp, os.ErrNotExist) {
		t.Fatalf("got error %v, expect %v", gotResp, os.ErrNotExist)
	}
}

func TestTree_doReadReq(t *testing.T) {
	tests := []struct {
		name     string
		testFunc func(t *testing.T)
	}{
		{
			"read success with offset 0",
			testTreeDoReadReqOffset0Success,
		},
		{
			"read success with offset 5",
			testTreeDoReadReqOffset5Success,
		},
		{
			"read success over length",
			testTreeDoReadReqOverLengthSuccess,
		},
		{
			"read not found",
			testTreeDoReadReqNotFound,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, tt.testFunc)
	}
}

func testTreeDoReadReqOffset0Success(t *testing.T) {
	root := os.TempDir()

	file, err := ioutil.TempFile(root, "")
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		name := file.Name()
		_ = file.Close()
		_ = os.Remove(name)
	}()

	content := []byte("test content")
	if _, err := file.WriteAt(content, 0); err != nil {
		t.Fatal(err)
	}

	tree := NewTree(root)
	tree.Run()
	defer tree.Shutdown()

	respCh := make(chan ReadResponse, 1)
	req := ReadRequest{
		Ctx:    context.Background(),
		Name:   filepath.Join("/", filepath.Base(file.Name())),
		Offset: 0,
		Size:   uint64(len(content)),
		RespCh: respCh,
	}

	select {
	default:
		t.Fatal("send read file request to fs tree failed, exit test")

	case tree.readCh <- req:
	}

	gotResp := <-respCh

	if gotResp.Err != nil {
		t.Fatal(err)
	}

	if gotResp.Offset != 0 {
		t.Errorf("got offset %d, expect %d", gotResp.Offset, 0)
	}

	if !bytes.Equal(content, gotResp.Data) {
		t.Fatalf("got data %v, expect data %v", gotResp.Data, content)
	}
}

func testTreeDoReadReqOffset5Success(t *testing.T) {
	root := os.TempDir()

	file, err := ioutil.TempFile(root, "")
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		name := file.Name()
		_ = file.Close()
		_ = os.Remove(name)
	}()

	content := []byte("test content")
	if _, err := file.WriteAt(content, 0); err != nil {
		t.Fatal(err)
	}

	tree := NewTree(root)
	tree.Run()
	defer tree.Shutdown()

	respCh := make(chan ReadResponse, 1)
	req := ReadRequest{
		Ctx:    context.Background(),
		Name:   filepath.Join("/", filepath.Base(file.Name())),
		Offset: 5,
		Size:   uint64(len(content) - 5),
		RespCh: respCh,
	}

	select {
	default:
		t.Fatal("send read file request to fs tree failed, exit test")

	case tree.readCh <- req:
	}

	gotResp := <-respCh

	if gotResp.Err != nil {
		t.Fatal(err)
	}

	if gotResp.Offset != 5 {
		t.Errorf("got offset %d, expect %d", gotResp.Offset, 5)
	}

	if !bytes.Equal(content[5:], gotResp.Data) {
		t.Fatalf("got data %v, expect data %v", gotResp.Data, content[5:])
	}
}

func testTreeDoReadReqOverLengthSuccess(t *testing.T) {
	root := os.TempDir()

	file, err := ioutil.TempFile(root, "")
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		name := file.Name()
		_ = file.Close()
		_ = os.Remove(name)
	}()

	content := []byte("test content")
	if _, err := file.WriteAt(content, 0); err != nil {
		t.Fatal(err)
	}

	tree := NewTree(root)
	tree.Run()
	defer tree.Shutdown()

	respCh := make(chan ReadResponse, 1)
	req := ReadRequest{
		Ctx:    context.Background(),
		Name:   filepath.Join("/", filepath.Base(file.Name())),
		Offset: 0,
		Size:   uint64(len(content) * 2),
		RespCh: respCh,
	}

	select {
	default:
		t.Fatal("send read file request to fs tree failed, exit test")

	case tree.readCh <- req:
	}

	gotResp := <-respCh

	if gotResp.Err != nil {
		t.Fatal(err)
	}

	if gotResp.Offset != 0 {
		t.Errorf("got offset %d, expect %d", gotResp.Offset, 0)
	}

	if !bytes.Equal(content, gotResp.Data) {
		t.Fatalf("got data %v, expect data %v", gotResp.Data, content)
	}
}

func testTreeDoReadReqNotFound(t *testing.T) {
	root := os.TempDir()

	name := "test-file"

	tree := NewTree(root)
	tree.Run()
	defer tree.Shutdown()

	respCh := make(chan ReadResponse, 1)
	req := ReadRequest{
		Ctx:    context.Background(),
		Name:   filepath.Join("/not-found", name),
		Offset: 0,
		Size:   10,
		RespCh: respCh,
	}

	select {
	default:
		t.Fatal("send read file request to fs tree failed, exit test")

	case tree.readCh <- req:
	}

	gotResp := <-respCh

	if len(gotResp.Data) > 0 {
		t.Fatalf("got data %v, expect no data", gotResp.Data)
	}

	if !errors.Is(gotResp.Err, os.ErrNotExist) {
		t.Fatalf("got error %v, expect error %v", gotResp.Err, os.ErrNotExist)
	}
}

func TestTree_doWriteReq(t *testing.T) {
	tests := []struct {
		name     string
		testFunc func(t *testing.T)
	}{
		{
			"write success with offset 0",
			testTreeDoWriteReqOffset0Success,
		},
		{
			"write success with offset 5",
			testTreeDoWriteReqOffset5Success,
		},
		{
			"write success not found",
			testTreeDoWriteReqNotFound,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, tt.testFunc)
	}
}

func testTreeDoWriteReqOffset0Success(t *testing.T) {
	root := os.TempDir()

	file, err := ioutil.TempFile(root, "")
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		name := file.Name()
		_ = file.Close()
		_ = os.Remove(name)
	}()

	tree := NewTree(root)
	tree.Run()
	defer tree.Shutdown()

	content := []byte("test content")
	respCh := make(chan WriteResponse, 1)
	req := WriteRequest{
		Ctx:    context.Background(),
		Name:   filepath.Join("/", filepath.Base(file.Name())),
		Offset: 0,
		Data:   content,
		RespCh: respCh,
	}

	select {
	default:
		t.Fatal("send write file request to fs tree failed, exit test")

	case tree.writeCh <- req:
	}

	gotResp := <-respCh

	if gotResp.Err != nil {
		t.Fatal(err)
	}

	if gotResp.Written != len(content) {
		t.Errorf("got written %d, expect %d", gotResp.Written, len(content))
	}

	gotData, err := ioutil.ReadAll(file)
	if err != nil {
		t.Fatal(err)
	}

	if !bytes.Equal(content, gotData) {
		t.Fatalf("got data %v, expect data %v", gotData, content)
	}
}

func testTreeDoWriteReqOffset5Success(t *testing.T) {
	root := os.TempDir()

	file, err := ioutil.TempFile(root, "")
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		name := file.Name()
		_ = file.Close()
		_ = os.Remove(name)
	}()

	tree := NewTree(root)
	tree.Run()
	defer tree.Shutdown()

	content := []byte("test content")

	if _, err := file.WriteAt(content, 0); err != nil {
		t.Fatal(err)
	}

	overrideData := []byte("new content")

	respCh := make(chan WriteResponse, 1)
	req := WriteRequest{
		Ctx:    context.Background(),
		Name:   filepath.Join("/", filepath.Base(file.Name())),
		Offset: 5,
		Data:   overrideData,
		RespCh: respCh,
	}

	select {
	default:
		t.Fatal("send write file request to fs tree failed, exit test")

	case tree.writeCh <- req:
	}

	gotResp := <-respCh

	if gotResp.Err != nil {
		t.Fatal(err)
	}

	if gotResp.Written != len(overrideData) {
		t.Errorf("got written %d, expect %d", gotResp.Written, len(overrideData))
	}

	gotData, err := ioutil.ReadAll(file)
	if err != nil {
		t.Fatal(err)
	}

	content = append(content[:5], overrideData...)

	if !bytes.Equal(content, gotData) {
		t.Fatalf("got data %v, expect data %v", gotData, content)
	}
}

func testTreeDoWriteReqNotFound(t *testing.T) {
	root := os.TempDir()

	name := "test-file"

	tree := NewTree(root)
	tree.Run()
	defer tree.Shutdown()

	content := []byte("test content")

	respCh := make(chan WriteResponse, 1)
	req := WriteRequest{
		Ctx:    context.Background(),
		Name:   filepath.Join("/not-found", name),
		Offset: 0,
		Data:   content,
		RespCh: respCh,
	}

	select {
	default:
		t.Fatal("send write file request to fs tree failed, exit test")

	case tree.writeCh <- req:
	}

	gotResp := <-respCh

	if gotResp.Written > 0 {
		t.Errorf("got written %d, expect 0", gotResp.Written)
	}

	if !errors.Is(gotResp.Err, os.ErrNotExist) {
		t.Fatalf("got error %v, expect error %v", gotResp.Err, os.ErrNotExist)
	}
}
