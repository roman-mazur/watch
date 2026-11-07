//go:build darwin || dragonfly || freebsd || netbsd || openbsd

package dirwatch

// kqueue keeps a file open for every entry of a watched directory: a large directory
// tree, like a checkout of Chromium, has more entries than a process may open
// (61440 on macOS), and the files are shared with the rest of the system.
const (
	defaultPollThreshold = 512
	defaultPollBudget    = 8192
)
