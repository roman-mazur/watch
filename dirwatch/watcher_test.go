package dirwatch

import (
	"maps"
	"os"
	"path/filepath"
	"slices"
	"testing"
	"time"
)

const testDelay = 20 * time.Millisecond

func newTestWatcher(t *testing.T) *Watcher {
	t.Helper()
	w, err := New(func(err error) { t.Errorf("watch error: %s", err) })
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = w.Close() })
	return w
}

func writeFile(t *testing.T, path, text string) {
	t.Helper()
	if err := os.WriteFile(path, []byte(text), 0o600); err != nil {
		t.Fatal(err)
	}
}

// waitFor returns the next value from the channel, or fails if there is none soon.
func waitFor[T any](t *testing.T, c <-chan T) T {
	t.Helper()
	select {
	case v := <-c:
		return v
	case <-time.After(5 * time.Second):
		t.Fatal("no change reported")
		panic("unreachable")
	}
}

// expectNone fails if a value comes from the channel in a while.
func expectNone[T any](t *testing.T, c <-chan T) {
	t.Helper()
	select {
	case v := <-c:
		t.Fatalf("unexpected change: %v", v)
	case <-time.After(10 * testDelay):
	}
}

func watchFile(t *testing.T, w *Watcher, path string) (<-chan struct{}, func()) {
	t.Helper()
	changes := make(chan struct{}, 10)
	stop, err := w.WatchFile(path, testDelay, func() { changes <- struct{}{} })
	if err != nil {
		t.Fatal(err)
	}
	return changes, stop
}

func TestWatcher_File(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "a.txt")
	writeFile(t, path, "1")
	w := newTestWatcher(t)
	changes, stop := watchFile(t, w, path)

	t.Run("write", func(t *testing.T) {
		writeFile(t, path, "2")
		waitFor(t, changes)
	})

	t.Run("replace", func(t *testing.T) {
		tmp := filepath.Join(dir, "a.tmp")
		writeFile(t, tmp, "3")
		if err := os.Rename(tmp, path); err != nil {
			t.Fatal(err)
		}
		waitFor(t, changes)
	})

	t.Run("events are grouped", func(t *testing.T) {
		for i := range 5 {
			writeFile(t, path, string(rune('a'+i)))
		}
		waitFor(t, changes)
		expectNone(t, changes)
	})

	t.Run("other files", func(t *testing.T) {
		writeFile(t, filepath.Join(dir, "b.txt"), "b")
		expectNone(t, changes)
	})

	t.Run("stop", func(t *testing.T) {
		stop()
		stop() // No-op.
		writeFile(t, path, "4")
		expectNone(t, changes)
		if dirs := watchedDirs(w); len(dirs) != 0 {
			t.Errorf("still watching %v", dirs)
		}
	})
}

// watchedDirs returns a copy of the watched directories with the subscriptions count.
func watchedDirs(w *Watcher) map[string]int {
	w.mu.Lock()
	defer w.mu.Unlock()
	return maps.Clone(w.dirs)
}

func TestWatcher_SharedDirectory(t *testing.T) {
	dir := t.TempDir()
	a, b := filepath.Join(dir, "a.txt"), filepath.Join(dir, "b.txt")
	w := newTestWatcher(t)
	changesA, stopA := watchFile(t, w, a)
	changesB, stopB := watchFile(t, w, b)
	if dirs := watchedDirs(w); dirs[dir] != 2 || len(dirs) != 1 {
		t.Fatalf("watched directories: %v", dirs)
	}

	writeFile(t, b, "b") // Created.
	waitFor(t, changesB)
	expectNone(t, changesA)

	stopA()
	writeFile(t, b, "bb")
	waitFor(t, changesB) // The directory is still watched for b.

	stopB()
	if dirs := watchedDirs(w); len(dirs) != 0 {
		t.Errorf("still watching %v", dirs)
	}
}

func TestWatcher_Tree(t *testing.T) {
	root := t.TempDir()
	for _, d := range []string{"sub", ".hidden"} {
		if err := os.Mkdir(filepath.Join(root, d), 0o700); err != nil {
			t.Fatal(err)
		}
	}
	w := newTestWatcher(t)
	changes := make(chan []string, 10)
	stop, err := w.WatchTree(root, testDelay, func(paths []string) { changes <- paths })
	if err != nil {
		t.Fatal(err)
	}

	t.Run("nested file", func(t *testing.T) {
		path := filepath.Join(root, "sub", "a.txt")
		writeFile(t, path, "a")
		if got := waitFor(t, changes); !slices.Contains(got, path) {
			t.Errorf("changed %v, want %s", got, path)
		}
	})

	t.Run("new directory", func(t *testing.T) {
		dir := filepath.Join(root, "new")
		if err := os.Mkdir(dir, 0o700); err != nil {
			t.Fatal(err)
		}
		waitFor(t, changes)
		path := filepath.Join(dir, "b.txt")
		writeFile(t, path, "b")
		if got := waitFor(t, changes); !slices.Contains(got, path) {
			t.Errorf("changed %v, want %s", got, path)
		}
	})

	t.Run("hidden directory", func(t *testing.T) {
		hidden := filepath.Join(root, ".hidden", "c.txt")
		// Watching a file there must not report its changes to the tree.
		fileChanges, stopFile := watchFile(t, w, hidden)
		defer stopFile()
		writeFile(t, hidden, "c")
		waitFor(t, fileChanges)
		expectNone(t, changes)
	})

	t.Run("removed directory", func(t *testing.T) {
		dir := filepath.Join(root, "new")
		if err := os.RemoveAll(dir); err != nil {
			t.Fatal(err)
		}
		waitFor(t, changes)
		if dirs := watchedDirs(w); dirs[dir] != 0 {
			t.Errorf("the removed directory is still watched: %v", dirs)
		}
	})

	t.Run("stop", func(t *testing.T) {
		stop()
		writeFile(t, filepath.Join(root, "d.txt"), "d")
		expectNone(t, changes)
		if dirs := watchedDirs(w); len(dirs) != 0 {
			t.Errorf("still watching %v", dirs)
		}
	})
}

func TestWatcher_MissingDirectory(t *testing.T) {
	w := newTestWatcher(t)
	if _, err := w.WatchFile(filepath.Join(t.TempDir(), "no", "a.txt"), testDelay, func() {}); err == nil {
		t.Error("no error watching a file in a missing directory")
	}
	if dirs := watchedDirs(w); len(dirs) != 0 {
		t.Errorf("watching %v", dirs)
	}
}
