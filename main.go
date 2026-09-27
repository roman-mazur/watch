// Command watch subscribes to the events in the specified directory and executes
// the provided command when there is a write-like event.
// Writes to the child directories including newly created are also handled.
// Usage:
//
//	watch . go test ./...
package main

import (
	"flag"
	"log"
	"os"
	"os/exec"
	"time"

	"rmazur.io/watch/dirwatch"
)

var verbose = flag.Bool("v", false, "verbose mode")

func main() {
	flag.Parse()
	watchPath := flag.Arg(0)
	if watchPath == "" {
		watchPath = "."
	}
	var args []string
	if flag.NArg() > 1 {
		args = flag.Args()[1:]
	}
	logf("watching %s", watchPath)
	logf("cmd: %s", args)

	failed := make(chan error, 1)
	w, err := dirwatch.New(func(err error) {
		select {
		case failed <- err:
		default:
		}
	})
	if err != nil {
		log.Fatal(err)
	}
	defer w.Close()

	_, err = w.WatchTree(watchPath, time.Second, func(paths []string) {
		logf("changed: %s", paths)
		execute(args)
	})
	if err != nil {
		log.Fatal(err)
	}
	log.Fatal(<-failed)
}

func logf(fmt string, args ...any) {
	if *verbose {
		log.Printf(fmt, args...)
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
