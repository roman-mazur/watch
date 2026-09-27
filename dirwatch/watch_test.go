package dirwatch

import (
	"path/filepath"
	"testing"
	"time"
)

func TestWatch(t *testing.T) {
	root := t.TempDir()
	signals := make(chan string)
	go func() {
		// Watch cannot be stopped: it runs until the test binary exits.
		if err := Watch(root, signals); err != nil {
			t.Errorf("Watch: %s", err)
		}
	}()

	// Watch starts asynchronously: write until a change is reported.
	path := filepath.Join(root, "a.txt")
	for i := 0; ; i++ {
		writeFile(t, path, string(rune('a'+i%26)))
		select {
		case got := <-signals:
			if got != path {
				t.Errorf("changed %s, want %s", got, path)
			}
			return
		case <-time.After(2 * time.Second):
			if i == 5 {
				t.Fatal("no change reported")
			}
		}
	}
}
