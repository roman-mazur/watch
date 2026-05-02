// Command watch subscribes to the events in the specified directory and executes
// the provided command when there is a write-like event.
// Writes to the child directories including newly created are also handled.
// Usage:
// 		watch . go test ./...
package main

import (
	"flag"
	"log"
	"os"
	"os/exec"
	"path/filepath"
	"time"

	"github.com/fsnotify/fsnotify"
)

var verbose = flag.Bool("v", false, "verbose mode")

func main() {
	flag.Parse()
	watchPath := flag.Arg(0)
	if watchPath == "" {
		watchPath = "."
	}
	logf("watching %s", watchPath)

	w, err := fsnotify.NewWatcher()
	if err != nil {
		panic(err)
	}
	defer w.Close()

	for _, wp := range collectWatchPaths(watchPath) {
		if err := w.Add(wp); err != nil {
			panic(err)
		}
	}

	signals := make(chan struct{})
	go func() {
		args := flag.Args()[1:]
		logf("cmd: %s", args)
		for range signals {
			execute(args)
		}
	}()

	processEvents(w, signals)
}

func logf(fmt string, args ...any) {
	if *verbose {
		log.Printf(fmt, args...)
	}
}

func collectWatchPaths(dir string) []string {
	res := []string{dir}
	entries, err := os.ReadDir(dir)
	if err != nil {
		return res
	}
	for _, entry := range entries {
		if entry.IsDir() {
			logf("expanding to %s", entry.Name())
		}
		res = append(res, collectWatchPaths(filepath.Join(dir, entry.Name()))...)
	}
	return res
}

func processEvents(w *fsnotify.Watcher, out chan struct{}) {
	defer close(out)
	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()

	trigered := false

	for {
		select {
		case e := <-w.Events:
			if *verbose {
				log.Println("event:", e)
			}
			if e.Has(fsnotify.Write) || e.Has(fsnotify.Rename) || e.Has(fsnotify.Create) {
				trigered = true
			}
			if e.Has(fsnotify.Create) {
				fi, err := os.Stat(e.Name)
				if err == nil && fi.IsDir() {
					w.Add(e.Name)
				}
			}
		case err := <-w.Errors:
			log.Println(err)
			return

		case <-ticker.C:
			if trigered {
				trigered = false
				out <- struct{}{}
			}
		}
	}
}

func execute(args []string) {
	if len(args) == 0 {
		return
	}
	cmd := exec.Command(args[0], args[1:]...)
	cmd.Stdin = os.Stdin
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	_ = cmd.Run()
}
