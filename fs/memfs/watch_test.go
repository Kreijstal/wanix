package memfs

import (
	"context"
	"testing"
	"time"

	"tractor.dev/wanix/fs"
)

func TestWatch(t *testing.T) {
	fsys := New()
	ctx, cancel := context.WithCancel(context.Background())
	all, err := fsys.Watch(ctx, "...")
	if err != nil {
		t.Fatal(err)
	}
	top, err := fsys.Watch(ctx, ".", "*.tmp")
	if err != nil {
		t.Fatal(err)
	}

	if err := fsys.Mkdir("dir", 0755); err != nil {
		t.Fatal(err)
	}
	if err := fs.WriteFile(fsys, "dir/a", []byte("hi"), 0644); err != nil {
		t.Fatal(err)
	}
	if err := fs.WriteFile(fsys, "x.tmp", []byte("hi"), 0644); err != nil {
		t.Fatal(err)
	}
	if err := fsys.Rename("dir/a", "dir/b"); err != nil {
		t.Fatal(err)
	}
	if err := fsys.Remove("dir/b"); err != nil {
		t.Fatal(err)
	}

	next := func(ch <-chan fs.Event) string {
		select {
		case e := <-ch:
			return e.Op + " " + e.Path
		case <-time.After(time.Second):
			return "none"
		}
	}
	for _, want := range []string{"create dir", "create dir/a", "write dir/a", "create x.tmp", "write x.tmp",
		"remove dir/a", "create dir/b", "remove dir/b"} {
		if got := next(all); got != want {
			t.Fatalf("...: got %q, want %q", got, want)
		}
	}
	// only what is directly in the top, x.tmp excluded
	if got := next(top); got != "create dir" {
		t.Fatalf(".: got %q", got)
	}
	if got := next(top); got != "none" {
		t.Fatalf(".: got %q, want nothing more", got)
	}

	cancel()
	if _, ok := <-all; ok {
		t.Fatal("channel open after cancel")
	}
}
