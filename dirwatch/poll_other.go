//go:build !(darwin || dragonfly || freebsd || netbsd || openbsd)

package dirwatch

// A watched directory takes a single watch however many entries it has: the directories
// are never polled.
const (
	defaultPollThreshold = 0
	defaultPollBudget    = 0
)
