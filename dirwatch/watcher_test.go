package dirwatch

import (
	"errors"
	"fmt"
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

// watchedDirs returns the watched and polled directories with the subscriptions count.
func watchedDirs(w *Watcher) map[string]int {
	w.mu.Lock()
	defer w.mu.Unlock()
	dirs := make(map[string]int, len(w.dirs))
	for dir, d := range w.dirs {
		dirs[dir] = d.subs
	}
	return dirs
}

// polledDirs returns the sorted polled directories and the cost of the watched ones.
func polledDirs(w *Watcher) (polled []string, cost int) {
	w.mu.Lock()
	defer w.mu.Unlock()
	for dir, d := range w.dirs {
		if d.polled {
			polled = append(polled, dir)
		}
	}
	slices.Sort(polled)
	return polled, w.cost
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

func TestWatcher_CloseTimeout(t *testing.T) {
	prev := closeTimeout
	closeTimeout = 10 * testDelay
	t.Cleanup(func() { closeTimeout = prev })

	w := newTestWatcher(t)
	// Simulate the fsnotify watcher that never finishes closing.
	block := make(chan struct{})
	t.Cleanup(func() { close(block) })
	w.closeFw = func() error {
		<-block
		return w.fw.Close()
	}
	if err := w.Close(); !errors.Is(err, errCloseTimeout) {
		t.Errorf("Close = %v, want %v", err, errCloseTimeout)
	}
}

func TestWatcher_Dir(t *testing.T) {
	dir := t.TempDir()
	sub := filepath.Join(dir, "sub")
	if err := os.Mkdir(sub, 0o700); err != nil {
		t.Fatal(err)
	}
	existing := filepath.Join(dir, "a.txt")
	writeFile(t, existing, "a")
	w := newTestWatcher(t)
	changes := make(chan []string, 10)
	stop, err := w.WatchDir(dir, testDelay, func(paths []string) { changes <- paths })
	if err != nil {
		t.Fatal(err)
	}
	expectChange := func(t *testing.T, path string) {
		t.Helper()
		if got := waitFor(t, changes); !slices.Contains(got, path) {
			t.Errorf("changed %v, want %s", got, path)
		}
	}

	t.Run("create", func(t *testing.T) {
		path := filepath.Join(dir, "b.txt")
		writeFile(t, path, "b")
		expectChange(t, path)
	})

	t.Run("write", func(t *testing.T) {
		writeFile(t, existing, "changed")
		expectNone(t, changes)
	})

	t.Run("subdirectory", func(t *testing.T) {
		writeFile(t, filepath.Join(sub, "c.txt"), "c")
		expectNone(t, changes)
	})

	t.Run("rename", func(t *testing.T) {
		renamed := filepath.Join(dir, "renamed.txt")
		if err := os.Rename(existing, renamed); err != nil {
			t.Fatal(err)
		}
		expectChange(t, renamed)
	})

	t.Run("remove", func(t *testing.T) {
		path := filepath.Join(dir, "b.txt")
		if err := os.Remove(path); err != nil {
			t.Fatal(err)
		}
		expectChange(t, path)
	})

	t.Run("stop", func(t *testing.T) {
		stop()
		writeFile(t, filepath.Join(dir, "d.txt"), "d")
		expectNone(t, changes)
		if dirs := watchedDirs(w); len(dirs) != 0 {
			t.Errorf("still watching %v", dirs)
		}
	})

	t.Run("missing", func(t *testing.T) {
		if _, err := w.WatchDir(filepath.Join(dir, "missing"), testDelay, func([]string) {}); err == nil {
			t.Error("watching a missing directory")
		}
	})
}

func newPollingWatcher(t *testing.T, threshold, budget int) *Watcher {
	t.Helper()
	w, err := newWatcher(func(err error) { t.Errorf("watch error: %s", err) },
		pollConfig{threshold: threshold, budget: budget, interval: testDelay})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = w.Close() })
	return w
}

// makeDir creates a directory with the files f0.txt, f1.txt, ... in it.
func makeDir(t *testing.T, dir string, files int) string {
	t.Helper()
	if err := os.MkdirAll(dir, 0o700); err != nil {
		t.Fatal(err)
	}
	for i := range files {
		writeFile(t, filepath.Join(dir, fmt.Sprintf("f%d.txt", i)), "0")
	}
	return dir
}

func TestWatcher_PollThreshold(t *testing.T) {
	dir := makeDir(t, t.TempDir(), 4)
	w := newPollingWatcher(t, 3, 100)
	path := filepath.Join(dir, "f0.txt")
	fileChanges, stopFile := watchFile(t, w, path)
	dirChanges := make(chan []string, 10)
	stopDir, err := w.WatchDir(dir, testDelay, func(paths []string) { dirChanges <- paths })
	if err != nil {
		t.Fatal(err)
	}
	if polled, cost := polledDirs(w); !slices.Equal(polled, []string{dir}) || cost != 0 {
		t.Fatalf("polled %q with the cost %d, want only %s", polled, cost, dir)
	}

	t.Run("write", func(t *testing.T) {
		writeFile(t, path, "11")
		waitFor(t, fileChanges)
		expectNone(t, dirChanges)
	})

	t.Run("replace", func(t *testing.T) {
		tmp := filepath.Join(dir, "f0.tmp")
		writeFile(t, tmp, "222")
		if err := os.Rename(tmp, path); err != nil {
			t.Fatal(err)
		}
		waitFor(t, fileChanges)
	})

	t.Run("other file", func(t *testing.T) {
		writeFile(t, filepath.Join(dir, "f1.txt"), "11")
		expectNone(t, fileChanges)
	})

	t.Run("create and remove", func(t *testing.T) {
		created := filepath.Join(dir, "new.txt")
		writeFile(t, created, "new")
		if got := waitFor(t, dirChanges); !slices.Contains(got, created) {
			t.Errorf("changed %v, want %s", got, created)
		}
		if err := os.Remove(created); err != nil {
			t.Fatal(err)
		}
		if got := waitFor(t, dirChanges); !slices.Contains(got, created) {
			t.Errorf("changed %v, want %s", got, created)
		}
	})

	t.Run("stop", func(t *testing.T) {
		stopFile()
		stopDir()
		writeFile(t, path, "3333")
		expectNone(t, fileChanges)
		if dirs := watchedDirs(w); len(dirs) != 0 {
			t.Errorf("still watching %v", dirs)
		}
	})
}

func TestWatcher_PollBudget(t *testing.T) {
	root := t.TempDir()
	a := makeDir(t, filepath.Join(root, "a"), 5) // The cost of 6 with the directory itself.
	b := makeDir(t, filepath.Join(root, "b"), 5)
	w := newPollingWatcher(t, 100, 10)
	watch := func(dir string) func() {
		t.Helper()
		stop, err := w.WatchDir(dir, testDelay, func([]string) {})
		if err != nil {
			t.Fatal(err)
		}
		return stop
	}
	check := func(t *testing.T, wantPolled []string, wantCost int) {
		t.Helper()
		if polled, cost := polledDirs(w); !slices.Equal(polled, wantPolled) || cost != wantCost {
			t.Errorf("polled %q with the cost %d, want %q with %d", polled, cost, wantPolled, wantCost)
		}
	}

	stopA := watch(a)
	stopB := watch(b)
	check(t, []string{b}, 6)

	stopA()
	check(t, []string{b}, 0) // A directory keeps being polled.
	stopB()
	stopB = watch(b)
	check(t, nil, 6)
	stopB()
	check(t, nil, 0)
}

func TestWatcher_PollTree(t *testing.T) {
	root := t.TempDir()
	sub := makeDir(t, filepath.Join(root, "sub"), 4)
	w := newPollingWatcher(t, 3, 100)
	changes := make(chan []string, 10)
	if _, err := w.WatchTree(root, testDelay, func(paths []string) { changes <- paths }); err != nil {
		t.Fatal(err)
	}

	t.Run("new directory in a polled one", func(t *testing.T) {
		dir := filepath.Join(sub, "new")
		if err := os.Mkdir(dir, 0o700); err != nil {
			t.Fatal(err)
		}
		waitFor(t, changes)
		path := filepath.Join(dir, "a.txt")
		writeFile(t, path, "a")
		if got := waitFor(t, changes); !slices.Contains(got, path) {
			t.Errorf("changed %v, want %s", got, path)
		}
	})

	t.Run("polled directory removed", func(t *testing.T) {
		if err := os.RemoveAll(sub); err != nil {
			t.Fatal(err)
		}
		waitFor(t, changes)
		if polled, _ := polledDirs(w); len(polled) != 0 {
			t.Errorf("still polling %q", polled)
		}
		if dirs := watchedDirs(w); len(dirs) != 1 {
			t.Errorf("watching %v, want only the root", dirs)
		}
	})
}
