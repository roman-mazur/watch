package dirwatch

import (
	"os"
	"path/filepath"
	"strings"
	"time"
)

// Watch sends the paths changed in the directory tree p to signals, see Watcher.WatchTree:
// the changes are grouped until there are none for a second. It blocks until the
// underlying watcher fails, closing signals then, and returns the error.
func Watch(p string, signals chan string) error {
	failed := make(chan error, 1)
	w, err := New(func(err error) {
		select {
		case failed <- err:
		default:
		}
	})
	if err != nil {
		return err
	}
	defer w.Close()

	stop, err := w.WatchTree(p, time.Second, func(paths []string) {
		for _, path := range paths {
			signals <- path
		}
	})
	if err != nil {
		return err
	}
	defer close(signals)
	defer stop()
	return <-failed
}

func collectWatchPaths(dir string) []string {
	res := []string{dir}
	entries, err := os.ReadDir(dir)
	if err != nil {
		return res
	}
	for _, entry := range entries {
		if entry.IsDir() && filterEntry(filepath.Base(entry.Name())) {
			res = append(res, collectWatchPaths(filepath.Join(dir, entry.Name()))...)
		}
	}
	return res
}

func filterEntry(name string) bool {
	return !strings.HasPrefix(name, ".")
}
