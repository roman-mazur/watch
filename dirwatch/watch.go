package dirwatch

import (
	"os"
	"path/filepath"
	"time"

	"github.com/fsnotify/fsnotify"
)

func Watch(p string, signals chan string) error {
	w, err := fsnotify.NewWatcher()
	if err != nil {
		return err
	}
	defer w.Close()

	for _, wp := range collectWatchPaths(p) {
		if err := w.Add(wp); err != nil {
			return err
		}
	}

	processEvents(w, signals)
	return nil
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

func processEvents(w *fsnotify.Watcher, out chan string) {
	defer close(out)
	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()

	var changedPaths []string

	for {
		select {
		case e := <-w.Events:
			if e.Has(fsnotify.Write) || e.Has(fsnotify.Rename) || e.Has(fsnotify.Create) || e.Has(fsnotify.Remove) {
				changedPaths = append(changedPaths, e.Name)
			}
			if e.Has(fsnotify.Create) {
				fi, err := os.Stat(e.Name)
				if err == nil && fi.IsDir() && filterEntry(filepath.Base(e.Name)) {
					_ = w.Add(e.Name)
				}
			}
		case <-w.Errors:
			return

		case <-ticker.C:
			if len(changedPaths) > 0 {
				for _, p := range changedPaths {
					out <- p
				}
				changedPaths = changedPaths[:0]
			}
		}
	}
}

func filterEntry(name string) bool {
	return !strings.HasPrefix(name, ".")
}
