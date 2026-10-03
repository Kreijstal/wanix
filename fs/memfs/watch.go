package memfs

import (
	"context"
	"path"
	"strings"
	"sync"

	"tractor.dev/wanix/fs"
)

var _ fs.WatchFS = (*FS)(nil)

// Ops in the events of Watch
const (
	OpCreate = "create" // a file, folder or symlink made
	OpWrite  = "write"  // a file's contents changed (closed after writing, or truncated)
	OpRemove = "remove" // removed, or renamed away (followed by a create for the new name)
	OpAttrib = "attrib" // mode, owner or times changed
)

type watcher struct {
	dir       string // "." for the top
	recursive bool
	exclude   []string
	mu        sync.Mutex
	queue     []fs.Event
	wake      chan struct{}
}

func (w *watcher) wants(name string) bool {
	if w.recursive {
		if w.dir != "." && name != w.dir && !strings.HasPrefix(name, w.dir+"/") {
			return false
		}
	} else if name != w.dir && path.Dir(name) != w.dir {
		return false
	}
	for _, pattern := range w.exclude {
		if ok, _ := path.Match(pattern, name); ok {
			return false
		}
		if ok, _ := path.Match(pattern, path.Base(name)); ok {
			return false
		}
	}
	return true
}

// Watch reports changes to name (a folder: it and what is directly in it), or
// to everything below it when name ends in "/..." ("..." alone for all).
// Exclude patterns are matched against the changed path and its last element.
// Events are queued without bound, so a slow reader loses none; the channel
// closes when ctx is done.
func (fsys *FS) Watch(ctx context.Context, name string, exclude ...string) (<-chan fs.Event, error) {
	w := &watcher{exclude: exclude, wake: make(chan struct{}, 1)}
	switch {
	case name == "...":
		w.dir, w.recursive = ".", true
	case strings.HasSuffix(name, "/..."):
		w.dir, w.recursive = path.Clean(strings.TrimSuffix(name, "/...")), true
	default:
		w.dir = path.Clean(name)
	}
	if !fs.ValidPath(w.dir) {
		return nil, &fs.PathError{Op: "watch", Path: name, Err: fs.ErrNotExist}
	}

	fsys.watchMu.Lock()
	if fsys.watchers == nil {
		fsys.watchers = make(map[*watcher]struct{})
	}
	fsys.watchers[w] = struct{}{}
	fsys.watchMu.Unlock()

	ch := make(chan fs.Event)
	go func() {
		defer close(ch)
		defer func() {
			fsys.watchMu.Lock()
			delete(fsys.watchers, w)
			fsys.watchMu.Unlock()
		}()
		for {
			w.mu.Lock()
			queue := w.queue
			w.queue = nil
			w.mu.Unlock()
			for _, e := range queue {
				select {
				case ch <- e:
				case <-ctx.Done():
					return
				}
			}
			select {
			case <-w.wake:
			case <-ctx.Done():
				return
			}
		}
	}()
	return ch, nil
}

// notify tells the watchers of a change to name (cleaned, relative to fsys)
func (fsys *FS) notify(op, name string) {
	fsys.watchMu.Lock()
	defer fsys.watchMu.Unlock()
	for w := range fsys.watchers {
		if !w.wants(name) {
			continue
		}
		w.mu.Lock()
		w.queue = append(w.queue, fs.Event{Path: name, Op: op})
		w.mu.Unlock()
		select {
		case w.wake <- struct{}{}:
		default:
		}
	}
}

// changed notifies when a change made no error (for the methods' defers)
func (fsys *FS) changed(err error, op, name string) {
	if err == nil {
		fsys.notify(op, name)
	}
}
