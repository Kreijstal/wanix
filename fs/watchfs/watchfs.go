// Package watchfs exposes the changes to a namespace as a file: each read
// gives one or more lines "<op> <path>\n" (paths from the namespace's top),
// waiting for the next change when there is none. Each open is its own
// watch, from that moment until it is closed.
package watchfs

import (
	"context"
	"io"
	iofs "io/fs"
	"os"
	"strings"
	"sync"

	"tractor.dev/wanix/fs"
	"tractor.dev/wanix/fs/fskit"
)

// FS is a single file, ".", watching everything below the top of ns that a
// filesystem bound there can watch (fs.WatchFS).
type FS struct {
	ns fs.FS
}

var (
	_ fs.FS            = (*FS)(nil)
	_ fs.RouteFS       = (*FS)(nil)
	_ fs.OpenFileFS    = (*FS)(nil)
	_ fs.OpenContextFS = (*FS)(nil)
)

func New(ns fs.FS) *FS {
	return &FS{ns: ns}
}

func (w *FS) Route(ctx context.Context, name string) (fs.FS, string, error) {
	if name == "." {
		return w, ".", nil
	}
	return nil, "", &fs.PathError{Op: "route", Path: name, Err: fs.ErrNotExist}
}

func (w *FS) Open(name string) (iofs.File, error) {
	return w.OpenContext(context.Background(), name)
}

func (w *FS) OpenContext(ctx context.Context, name string) (fs.File, error) {
	return w.OpenFile(name, os.O_RDONLY, 0)
}

func (w *FS) OpenFile(name string, flag int, perm fs.FileMode) (fs.File, error) {
	if name != "." {
		return nil, &fs.PathError{Op: "open", Path: name, Err: fs.ErrNotExist}
	}
	if flag&0x3 != os.O_RDONLY {
		return nil, &fs.PathError{Op: "open", Path: name, Err: fs.ErrPermission}
	}
	ctx, cancel := context.WithCancel(context.Background())
	return &file{ns: w.ns, ctx: ctx, cancel: cancel}, nil
}

type file struct {
	ns     fs.FS
	ctx    context.Context
	cancel context.CancelFunc
	once   sync.Once
	events <-chan fs.Event
	err    error
	prefix string // where the watched filesystem is in it, "" for its top
	mu     sync.Mutex
	buf    []byte
}

func (f *file) start() {
	f.once.Do(func() {
		wfs, rel, err := fs.ResolveTo[fs.WatchFS](f.ns, f.ctx, ".")
		if err != nil {
			f.err = err
			return
		}
		watched := "..."
		if rel != "." {
			watched = rel + "/..."
			f.prefix = rel + "/"
		}
		f.events, f.err = wfs.Watch(f.ctx, watched)
	})
}

func (f *file) line(e fs.Event) string {
	p := strings.TrimPrefix(e.Path, f.prefix)
	if p == strings.TrimSuffix(f.prefix, "/") {
		p = "."
	}
	return e.Op + " " + p + "\n"
}

func (f *file) Read(p []byte) (int, error) {
	f.start()
	if f.err != nil {
		return 0, f.err
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	if len(f.buf) == 0 {
		e, ok := <-f.events
		if !ok {
			return 0, io.EOF
		}
		f.buf = append(f.buf, f.line(e)...)
		// and what else is there already, in the same read
		for more := true; more && len(f.buf) < len(p); {
			select {
			case e, ok := <-f.events:
				if !ok {
					more = false
				} else {
					f.buf = append(f.buf, f.line(e)...)
				}
			default:
				more = false
			}
		}
	}
	n := copy(p, f.buf)
	f.buf = f.buf[n:]
	return n, nil
}

func (f *file) Write(b []byte) (int, error) {
	return 0, fs.ErrPermission
}

func (f *file) Close() error {
	f.cancel()
	return nil
}

func (f *file) Stat() (fs.FileInfo, error) {
	return fskit.Entry("watch", 0444), nil
}
