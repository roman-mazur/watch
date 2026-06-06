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

	"rmazur.io/watch/dirwatch"
)

var verbose = flag.Bool("v", false, "verbose mode")

func main() {
	flag.Parse()
	watchPath := flag.Arg(0)
	if watchPath == "" {
		watchPath = "."
	}
	logf("watching %s", watchPath)

	signals := make(chan string)
	go func() {
		err := dirwatch.Watch(watchPath, signals)
		if err != nil {
			panic(err)
		}
	}()

	args := flag.Args()[1:]
	logf("cmd: %s", args)
	for range signals {
		execute(args)
	}
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
