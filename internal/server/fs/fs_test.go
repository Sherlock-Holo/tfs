package fs

import (
	"bytes"
	"context"
	"io/ioutil"
	"math/rand"
	"os"
	"path/filepath"
	"reflect"
	"sort"
	"syscall"
	"testing"
	"time"

	"github.com/Sherlock-Holo/tfs/api/rpc"
	"github.com/Sherlock-Holo/tfs/internal"
	errors "golang.org/x/xerrors"
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
		t.Fatal(gotResp.Err)
	}

	expectAttr := internal.CreateAttr(info)

	if !reflect.DeepEqual(gotResp.Attr, expectAttr) {
		t.Errorf("attr not equal, got %v, expect %v", gotResp.Attr, expectAttr)
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
		t.Errorf("got error %v, expect got error %v", gotResp.Err, os.ErrNotExist)
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
		t.Fatal(gotResp.Err)
	}

	expectAttr := internal.CreateAttr(info)

	if !reflect.DeepEqual(gotResp.Attr, expectAttr) {
		t.Errorf("attr not equal, got %v, expect %v", gotResp.Attr, expectAttr)
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
		t.Fatal(err)
	}

	if gotResp.Err != nil {
		t.Fatal(gotResp.Err)
	}

	expectAttr := internal.CreateAttr(info)

	if !reflect.DeepEqual(gotResp.Attr, expectAttr) {
		t.Errorf("attr not equal, got %v, expect %v", gotResp.Attr, expectAttr)
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
		t.Errorf("expect error %v, got error %v", gotResp.Err, os.ErrNotExist)
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
		t.Errorf("expect error %v, got error %v", gotResp.Err, os.ErrNotExist)
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
		t.Fatal(err)
	}

	if gotResp.Err != nil {
		t.Fatal(gotResp.Err)
	}

	expectAttr := internal.CreateAttr(info)

	if !reflect.DeepEqual(gotResp.Attr, expectAttr) {
		t.Errorf("attr not equal, got %v, expect %v", gotResp.Attr, expectAttr)
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
		t.Errorf("got error %v, expect got error %v", gotResp.Err, os.ErrNotExist)
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
		t.Errorf("got error %v, expect got error %v", gotResp.Err, os.ErrNotExist)
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
		Mode:    0700 | os.ModeDir,
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
		t.Fatal(err)
	}

	if gotResp.Err != nil {
		t.Fatal(gotResp.Err)
	}

	expectAttr := internal.CreateAttr(info)
	if !reflect.DeepEqual(gotResp.Attr, expectAttr) {
		t.Errorf("attr not equal, got %v, expect %v", gotResp.Attr, expectAttr)
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
		Mode:    0700 | os.ModeDir,
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
		t.Errorf("got error %v, expect got error %v", gotResp.Err, os.ErrNotExist)
	}
}

func testTreeDoMkdirReqParentNotExist(t *testing.T) {
	root := os.TempDir()

	name := "test-dir"
	if err := os.Mkdir(filepath.Join(root, name), os.ModeDir|0600); err != nil {
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
		DirPath: "/not-exist",
		Name:    name,
		Mode:    0700 | os.ModeDir,
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
		t.Errorf("got error %v, expect got error %v", gotResp.Err, os.ErrNotExist)
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
		t.Fatal(gotResp)
	}

	attr := internal.CreateAttr(info)

	if uint64(attr.Size) != req.Offset+req.Size {
		t.Errorf("got size %d, expect %d", attr.Size, req.Offset+req.Size)
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
		t.Fatal(gotResp)
	}

	attr := internal.CreateAttr(info)

	if uint64(attr.Size) != req.Offset+req.Size {
		t.Errorf("got size %d, expect %d", attr.Size, req.Offset+req.Size)
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
		t.Errorf("got error %v, expect %v", gotResp, os.ErrNotExist)
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
		t.Fatal(gotResp.Err)
	}

	if gotResp.Offset != 0 {
		t.Errorf("got offset %d, expect %d", gotResp.Offset, 0)
	}

	if !bytes.Equal(content, gotResp.Data) {
		t.Errorf("got data %v, expect data %v", gotResp.Data, content)
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
		t.Fatal(gotResp.Err)
	}

	if gotResp.Offset != 5 {
		t.Errorf("got offset %d, expect %d", gotResp.Offset, 5)
	}

	if !bytes.Equal(content[5:], gotResp.Data) {
		t.Errorf("got data %v, expect data %v", gotResp.Data, content[5:])
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
		t.Fatal(gotResp.Err)
	}

	if gotResp.Offset != 0 {
		t.Errorf("got offset %d, expect %d", gotResp.Offset, 0)
	}

	if !bytes.Equal(content, gotResp.Data) {
		t.Errorf("got data %v, expect data %v", gotResp.Data, content)
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
		t.Errorf("got error %v, expect error %v", gotResp.Err, os.ErrNotExist)
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
		t.Fatal(gotResp.Err)
	}

	if gotResp.Written != len(content) {
		t.Errorf("got written %d, expect %d", gotResp.Written, len(content))
	}

	gotData, err := ioutil.ReadAll(file)
	if err != nil {
		t.Fatal(err)
	}

	if !bytes.Equal(content, gotData) {
		t.Errorf("got data %v, expect data %v", gotData, content)
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
		t.Fatal(gotResp.Err)
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
		t.Errorf("got data %v, expect data %v", gotData, content)
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
		t.Errorf("got error %v, expect error %v", gotResp.Err, os.ErrNotExist)
	}
}

func TestTree_doDeleteFileReq(t *testing.T) {
	tests := []struct {
		name     string
		testFunc func(t *testing.T)
	}{
		{
			"delete file success",
			testTreeDoDeleteSuccess,
		},
		{
			"delete file not found",
			testTreeDoDeleteNotFound,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, tt.testFunc)
	}
}

func testTreeDoDeleteSuccess(t *testing.T) {
	root := os.TempDir()

	file, err := ioutil.TempFile(root, "")
	if err != nil {
		t.Fatal(err)
	}

	name := file.Name()
	_ = file.Close()

	defer func() {
		_ = os.Remove(name)
	}()

	tree := NewTree(root)
	tree.Run()
	defer tree.Shutdown()

	respCh := make(chan error, 1)
	req := DeleteFileRequest{
		Ctx:    context.Background(),
		Name:   filepath.Join("/", filepath.Base(name)),
		RespCh: respCh,
	}

	select {
	default:
		t.Fatal("send delete file request to fs tree failed, exit test")

	case tree.deleteFileCh <- req:
	}

	gotResp := <-respCh

	if gotResp != nil {
		t.Fatal(gotResp)
	}

	_, err = os.Stat(name)
	if !errors.Is(err, os.ErrNotExist) {
		t.Errorf("got error %v, expect error %v", err, os.ErrNotExist)
	}
}

func testTreeDoDeleteNotFound(t *testing.T) {
	root := os.TempDir()

	tree := NewTree(root)
	tree.Run()
	defer tree.Shutdown()

	respCh := make(chan error, 1)
	req := DeleteFileRequest{
		Ctx:    context.Background(),
		Name:   filepath.Join("/not-found"),
		RespCh: respCh,
	}

	select {
	default:
		t.Fatal("send delete file request to fs tree failed, exit test")

	case tree.deleteFileCh <- req:
	}

	gotResp := <-respCh

	if !errors.Is(gotResp, os.ErrNotExist) {
		t.Errorf("got error %v, expect error %v", gotResp, os.ErrNotExist)
	}
}

func TestTree_doRmdirReq(t *testing.T) {
	tests := []struct {
		name     string
		testFunc func(t *testing.T)
	}{
		{
			"rmdir success",
			testTreeDoRmdirSuccess,
		},
		{
			"rmdir not empty",
			testTreeDoRmdirNotEmpty,
		},
		{
			"rmdir not found",
			testTreeDoRmdirNotFound,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, tt.testFunc)
	}
}

func testTreeDoRmdirSuccess(t *testing.T) {
	root := os.TempDir()

	name, err := ioutil.TempDir(root, "")
	if err != nil {
		t.Fatal(err)
	}

	defer func() {
		_ = os.Remove(name)
	}()

	tree := NewTree(root)
	tree.Run()
	defer tree.Shutdown()

	respCh := make(chan error, 1)
	req := RmdirRequest{
		Ctx:    context.Background(),
		Name:   filepath.Join("/", filepath.Base(name)),
		RespCh: respCh,
	}

	select {
	default:
		t.Fatal("send rmdir request to fs tree failed, exit test")

	case tree.rmdirCh <- req:
	}

	gotResp := <-respCh

	if gotResp != nil {
		t.Fatal(gotResp)
	}

	_, err = os.Stat(name)
	if !errors.Is(err, os.ErrNotExist) {
		t.Errorf("got error %v, expect error %v", err, os.ErrNotExist)
	}
}

func testTreeDoRmdirNotEmpty(t *testing.T) {
	root := os.TempDir()

	name, err := ioutil.TempDir(root, "")
	if err != nil {
		t.Fatal(err)
	}

	file, err := ioutil.TempFile(name, "")
	if err != nil {
		t.Fatal(err)
	}

	defer func() {
		_ = file.Close()
		_ = os.RemoveAll(name)
	}()

	tree := NewTree(root)
	tree.Run()
	defer tree.Shutdown()

	respCh := make(chan error, 1)
	req := RmdirRequest{
		Ctx:    context.Background(),
		Name:   filepath.Join("/", filepath.Base(name)),
		RespCh: respCh,
	}

	select {
	default:
		t.Fatal("send rmdir request to fs tree failed, exit test")

	case tree.rmdirCh <- req:
	}

	gotResp := <-respCh

	if !errors.Is(gotResp, syscall.ENOTEMPTY) {
		t.Errorf("got error %v, expect error %v", gotResp, syscall.ENOTEMPTY)
	}
}

func testTreeDoRmdirNotFound(t *testing.T) {
	root := os.TempDir()

	tree := NewTree(root)
	tree.Run()
	defer tree.Shutdown()

	respCh := make(chan error, 1)
	req := RmdirRequest{
		Ctx:    context.Background(),
		Name:   filepath.Join("/not-found"),
		RespCh: respCh,
	}

	select {
	default:
		t.Fatal("send rmdir request to fs tree failed, exit test")

	case tree.rmdirCh <- req:
	}

	gotResp := <-respCh

	if !errors.Is(gotResp, os.ErrNotExist) {
		t.Fatalf("got error %v, expect error %v", gotResp, os.ErrNotExist)
	}
}

func TestTree_doReadDirReq(t *testing.T) {
	tests := []struct {
		name     string
		testFunc func(t *testing.T)
	}{
		{
			"read dir empty success",
			testTreeDoReadDirEmptySuccess,
		},
		{
			"read dir has sub dir and file success",
			testTreeDoReadDirHasSubDirAndFileSuccess,
		},
		{
			"read dir not exist",
			testTreeDoReadDirNotExist,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, tt.testFunc)
	}
}

func testTreeDoReadDirEmptySuccess(t *testing.T) {
	root := os.TempDir()

	name, err := ioutil.TempDir(root, "")
	if err != nil {
		t.Fatal(err)
	}

	defer func() {
		_ = os.Remove(name)
	}()

	tree := NewTree(root)
	tree.Run()
	defer tree.Shutdown()

	respCh := make(chan ReadDirResponse, 1)
	req := ReadDirRequest{
		Ctx:    context.Background(),
		Name:   filepath.Join("/", filepath.Base(name)),
		RespCh: respCh,
	}

	select {
	default:
		t.Fatal("send read dir request to fs tree failed, exit test")

	case tree.readDirCh <- req:
	}

	gotResp := <-respCh

	if gotResp.Err != nil {
		t.Fatal(gotResp.Err)
	}

	if len(gotResp.Entries) != 0 {
		t.Errorf("got entries %v, expect no entry", gotResp.Entries)
	}
}

func testTreeDoReadDirHasSubDirAndFileSuccess(t *testing.T) {
	root := os.TempDir()

	name, err := ioutil.TempDir(root, "")
	if err != nil {
		t.Fatal(err)
	}

	tempFile, err := ioutil.TempFile(name, "")
	if err != nil {
		t.Fatal(err)
	}

	tempFileInfo, err := tempFile.Stat()
	if err != nil {
		t.Fatal(err)
	}

	tempDir, err := ioutil.TempDir(name, "")
	if err != nil {
		t.Fatal(err)
	}

	tempDirInfo, err := os.Stat(tempDir)
	if err != nil {
		t.Fatal(err)
	}

	defer func() {
		_ = os.RemoveAll(name)
	}()

	tree := NewTree(root)
	tree.Run()
	defer tree.Shutdown()

	respCh := make(chan ReadDirResponse, 1)
	req := ReadDirRequest{
		Ctx:    context.Background(),
		Name:   filepath.Join("/", filepath.Base(name)),
		RespCh: respCh,
	}

	select {
	default:
		t.Fatal("send read dir request to fs tree failed, exit test")

	case tree.readDirCh <- req:
	}

	gotResp := <-respCh

	if gotResp.Err != nil {
		t.Fatal(gotResp.Err)
	}

	sort.Slice(gotResp.Entries, func(i, j int) bool {
		if gotResp.Entries[i].Type == rpc.EntryType_dir && gotResp.Entries[j].Type == rpc.EntryType_file {
			return true
		}
		return false
	})

	if len(gotResp.Entries) != 2 {
		t.Fatalf("got entries number %d, expect 2", len(gotResp.Entries))
	}

	if gotResp.Entries[0].Name != filepath.Base(tempDir) {
		t.Errorf("got dir entry name %s, expect %s", gotResp.Entries[0].Name, filepath.Base(tempDir))
	}

	if gotResp.Entries[0].Type != rpc.EntryType_dir {
		t.Errorf("got dir entry type %s, expect %s", gotResp.Entries[0].Type, rpc.EntryType_dir)
	}

	if gotResp.Entries[0].Mode != uint32(tempDirInfo.Mode()) {
		t.Errorf("got dir entry mode %v, expect %v", gotResp.Entries[0].Mode, uint32(tempDirInfo.Mode()))
	}

	if gotResp.Entries[1].Name != filepath.Base(tempFile.Name()) {
		t.Errorf("got file entry name %s, expect %s", gotResp.Entries[1].Name, filepath.Base(tempDir))
	}

	if gotResp.Entries[1].Type != rpc.EntryType_file {
		t.Errorf("got file entry type %s, expect %s", gotResp.Entries[1].Type, rpc.EntryType_file)
	}

	if gotResp.Entries[1].Mode != uint32(tempFileInfo.Mode()) {
		t.Errorf("got file entry mode %v, expect %v", gotResp.Entries[1].Mode, uint32(tempFileInfo.Mode()))
	}
}

func testTreeDoReadDirNotExist(t *testing.T) {
	root := os.TempDir()

	tree := NewTree(root)
	tree.Run()
	defer tree.Shutdown()

	respCh := make(chan ReadDirResponse, 1)
	req := ReadDirRequest{
		Ctx:    context.Background(),
		Name:   "/not-exist-dir",
		RespCh: respCh,
	}

	select {
	default:
		t.Fatal("send read dir request to fs tree failed, exit test")

	case tree.readDirCh <- req:
	}

	gotResp := <-respCh

	if len(gotResp.Entries) != 0 {
		t.Fatalf("got entries %v, expect no entry", gotResp.Entries)
	}

	if !errors.Is(gotResp.Err, os.ErrNotExist) {
		t.Errorf("got error %v, expect error %v", gotResp.Entries, os.ErrNotExist)
	}
}

func TestTree_doRenameReq(t *testing.T) {
	tests := []struct {
		name     string
		testFunc func(t *testing.T)
	}{
		{
			"rename only rename success",
			testTreeDoRenameFileSuccess,
		},
		{
			"rename move to new dir success",
			testTreeDoRenameFileMoveToNewDirSuccess,
		},
		// TODO error
	}

	for _, tt := range tests {
		t.Run(tt.name, tt.testFunc)
	}
}

func testTreeDoRenameFileSuccess(t *testing.T) {
	root, err := ioutil.TempDir(os.TempDir(), "")
	if err != nil {
		t.Fatal(err)
	}

	file, err := ioutil.TempFile(root, "")
	if err != nil {
		t.Fatal(err)
	}

	name := file.Name()
	_ = file.Close()

	defer func() {
		_ = os.Remove(filepath.Join(root, filepath.Base(name)+"rename"))
	}()

	tree := NewTree(root)
	tree.Run()
	defer tree.Shutdown()

	respCh := make(chan error, 1)
	req := RenameRequest{
		Ctx:        context.Background(),
		OldDirPath: "/",
		OldName:    filepath.Base(name),
		RespCh:     respCh,
		NewDirPath: "/",
		NewName:    filepath.Base(name) + "rename",
	}

	select {
	default:
		t.Fatal("send rename request to fs tree failed, exit test")

	case tree.renameCh <- req:
	}

	gotResp := <-respCh

	if gotResp != nil {
		t.Errorf("got error %v, expect nil", gotResp)
	}
}

func testTreeDoRenameFileMoveToNewDirSuccess(t *testing.T) {
	root, err := ioutil.TempDir(os.TempDir(), "")
	if err != nil {
		t.Fatal(err)
	}

	newPath := filepath.Join(root, "new-path")
	if err := os.Mkdir(newPath, 0755|os.ModeDir); err != nil {
		t.Fatal(err)
	}

	file, err := ioutil.TempFile(root, "")
	if err != nil {
		t.Fatal(err)
	}

	name := file.Name()
	_ = file.Close()

	defer func() {
		_ = os.Remove(filepath.Join(newPath, filepath.Base(name)+"rename"))
	}()

	tree := NewTree(root)
	tree.Run()
	defer tree.Shutdown()

	respCh := make(chan error, 1)
	req := RenameRequest{
		Ctx:        context.Background(),
		OldDirPath: "/",
		OldName:    filepath.Base(name),
		RespCh:     respCh,
		NewDirPath: filepath.Join("/", filepath.Base(newPath)),
		NewName:    filepath.Base(name) + "rename",
	}

	select {
	default:
		t.Fatal("send rename request to fs tree failed, exit test")

	case tree.renameCh <- req:
	}

	gotResp := <-respCh

	if gotResp != nil {
		t.Errorf("got error %v, expect nil", gotResp)
	}
}

func TestTree_doOpenFileReq(t *testing.T) {
	tests := []struct {
		name     string
		testFunc func(t *testing.T)
	}{
		{
			"open file success",
			testTreeDoOpenFileSuccess,
		},
		{
			"open file not exist",
			testTreeDoOpenFileNotExist,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, tt.testFunc)
	}
}

func testTreeDoOpenFileSuccess(t *testing.T) {
	root, err := ioutil.TempDir(os.TempDir(), "")
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		_ = os.RemoveAll(root)
	}()

	file, err := ioutil.TempFile(root, "")
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		_ = file.Close()
	}()

	tree := NewTree(root)
	tree.Run()
	defer tree.Shutdown()

	respCh := make(chan OpenFileResponse, 1)
	req := OpenFileRequest{
		Ctx:    context.Background(),
		Name:   filepath.Join("/", filepath.Base(file.Name())),
		RespCh: respCh,
	}

	select {
	default:
		t.Fatal("send open file request to fs tree failed, exit test")

	case tree.openFileCh <- req:
	}

	gotResp := <-respCh

	if gotResp.Err != nil {
		t.Fatal(gotResp.Err)
	}

	info, err := file.Stat()
	if err != nil {
		t.Fatal(err)
	}

	expectAttr := internal.CreateAttr(info)
	if !reflect.DeepEqual(gotResp.Attr, expectAttr) {
		t.Errorf("attr not equal, got %v, expect %v", gotResp.Attr, expectAttr)
	}
}

func testTreeDoOpenFileNotExist(t *testing.T) {
	root, err := ioutil.TempDir(os.TempDir(), "")
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		_ = os.RemoveAll(root)
	}()

	tree := NewTree(root)
	tree.Run()
	defer tree.Shutdown()

	respCh := make(chan OpenFileResponse, 1)
	req := OpenFileRequest{
		Ctx:    context.Background(),
		Name:   filepath.Join("/", "not-exist"),
		RespCh: respCh,
	}

	select {
	default:
		t.Fatal("send open file request to fs tree failed, exit test")

	case tree.openFileCh <- req:
	}

	gotResp := <-respCh

	if gotResp.Attr != nil {
		t.Errorf("got attr %v, expect nil", gotResp.Attr)
	}

	if !errors.Is(gotResp.Err, os.ErrNotExist) {
		t.Errorf("got error %v, expect %v", gotResp.Err, os.ErrNotExist)
	}
}
