// Package dirwatch watches the changes of files and directory trees.
package dirwatch

import (
	"errors"
	"io/fs"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"sync"
	"time"

	"github.com/fsnotify/fsnotify"
)

// Watcher shares a single fsnotify watcher among the subscriptions to the changes
// of files and directory trees, which can be stopped independently. A directory is
// watched once however many subscriptions need it.
//
// The callbacks of the subscriptions are called one at a time on the goroutine of
// the Watcher, after the events stop coming for the delay of the subscription.
//
// On macOS and BSD, fsnotify uses kqueue, which keeps a file open for every entry of
// a watched directory. A directory with more than 512 entries, or one that would take
// the entries of the watched directories over 8192, is listed every 2 seconds instead:
// its changes are reported later, and so are the writes of its files, detected by their
// size and modification time. A watched directory is polled from the moment it grows
// over these limits.
type Watcher struct {
	fw      *fsnotify.Watcher
	closeFw func() error // fw.Close, replaced in tests
	onError func(error)
	poll    pollConfig

	mu   sync.Mutex
	dirs map[string]*watchedDir // the directories the subscriptions need
	cost int                    // the open files taken by the watched directories, see pollConfig
	subs []*subscription
}

// pollConfig selects the directories to poll instead of watching them with fsnotify.
type pollConfig struct {
	threshold int // poll the directories with more entries, never poll if zero
	budget    int // poll the directories taking the cost of the watched ones over it
	interval  time.Duration
}

// watchedDir is a directory needed by the subscriptions.
type watchedDir struct {
	subs int // the subscriptions using it

	polled  bool
	cost    int              // the open files the fsnotify watch takes: the entries and the directory
	listing map[string]entry // the entries found when the polled directory was listed last time
}

// entry is the state of a directory entry compared by polling.
type entry struct {
	dir     bool
	size    int64
	modTime time.Time
}

type subscription struct {
	path    string // the file, the directory, or the root of the tree
	tree    bool
	dir     bool // the entries of the directory, not its subdirectories
	delay   time.Duration
	changed func(paths []string)
	dirs    map[string]bool // the directories the subscription needs watched

	// The changes waiting for the delay to pass.
	pending []string
	due     time.Time
}

// New starts a Watcher. The errors of the underlying watcher are passed to onError,
// which may be nil.
func New(onError func(error)) (*Watcher, error) {
	return newWatcher(onError, pollConfig{
		threshold: defaultPollThreshold,
		budget:    defaultPollBudget,
		interval:  2 * time.Second,
	})
}

func newWatcher(onError func(error), poll pollConfig) (*Watcher, error) {
	fw, err := fsnotify.NewWatcher()
	if err != nil {
		return nil, err
	}
	w := &Watcher{
		fw:      fw,
		closeFw: fw.Close,
		onError: onError,
		poll:    poll,
		dirs:    make(map[string]*watchedDir),
	}
	go w.run()
	return w, nil
}

// closeTimeout limits how long Close waits for the fsnotify watcher to stop.
var closeTimeout = 5 * time.Second

// errCloseTimeout is returned by Close when the fsnotify watcher does not stop in time.
var errCloseTimeout = errors.New("dirwatch: timed out closing the watcher")

// Close stops watching. The callback that is being called may still complete.
//
// On Windows, fsnotify (as of v1.10.1) loses the close request when it reports
// an error at the same time, and its Close never returns. So Close gives up
// after a timeout, leaving the fsnotify goroutines behind.
func (w *Watcher) Close() error {
	res := make(chan error, 1)
	go func() { res <- w.closeFw() }()
	select {
	case err := <-res:
		return err
	case <-time.After(closeTimeout):
		return errCloseTimeout
	}
}

// WatchFile calls changed after the file is written, or created, for example, when
// it's replaced by renaming another file to its name. The file's directory is watched,
// so the file does not have to exist.
//
// The callback may still be called once after stop returns, if the change was
// detected before.
func (w *Watcher) WatchFile(path string, delay time.Duration, changed func()) (stop func(), err error) {
	path, err = filepath.Abs(path)
	if err != nil {
		return nil, err
	}
	return w.subscribe(&subscription{
		path:    path,
		delay:   delay,
		changed: func([]string) { changed() },
	}, []string{filepath.Dir(path)})
}

// WatchTree calls changed with the paths that were written, created, removed, or renamed
// in the directory or its subdirectories, except the ones starting with a dot.
// The directories created later are watched too.
//
// The callback may still be called once after stop returns, if the change was
// detected before.
func (w *Watcher) WatchTree(dir string, delay time.Duration, changed func(paths []string)) (stop func(), err error) {
	dir, err = filepath.Abs(dir)
	if err != nil {
		return nil, err
	}
	return w.subscribe(&subscription{
		path:    dir,
		tree:    true,
		delay:   delay,
		changed: changed,
	}, collectWatchPaths(dir))
}

// WatchDir calls changed with the paths of the entries that were created, removed, or renamed
// in the directory: the changes of its listing. The subdirectories are not watched, and
// neither are the writes to the files.
//
// The callback may still be called once after stop returns, if the change was
// detected before.
func (w *Watcher) WatchDir(dir string, delay time.Duration, changed func(paths []string)) (stop func(), err error) {
	dir, err = filepath.Abs(dir)
	if err != nil {
		return nil, err
	}
	return w.subscribe(&subscription{
		path:    dir,
		dir:     true,
		delay:   delay,
		changed: changed,
	}, []string{dir})
}

func (w *Watcher) subscribe(s *subscription, dirs []string) (stop func(), err error) {
	s.dirs = make(map[string]bool, len(dirs))

	w.mu.Lock()
	defer w.mu.Unlock()
	for i, d := range dirs {
		if err := w.addDir(s, d); err != nil && i == 0 {
			// The first directory is required, the nested ones may be gone already.
			w.removeDirs(s)
			return nil, err
		}
	}
	w.subs = append(w.subs, s)

	var once sync.Once
	return func() {
		once.Do(func() {
			w.mu.Lock()
			defer w.mu.Unlock()
			w.subs = slices.DeleteFunc(w.subs, func(x *subscription) bool { return x == s })
			w.removeDirs(s)
		})
	}, nil
}

// addDir starts watching the directory for the subscription.
func (w *Watcher) addDir(s *subscription, dir string) error {
	if s.dirs[dir] {
		return nil
	}
	d := w.dirs[dir]
	if d == nil {
		var err error
		if d, err = w.startDir(dir); err != nil {
			return err
		}
		w.dirs[dir] = d
	}
	d.subs++
	s.dirs[dir] = true
	return nil
}

// startDir watches the directory with fsnotify, or starts polling it if it has too many entries.
func (w *Watcher) startDir(dir string) (*watchedDir, error) {
	if w.poll.threshold <= 0 {
		return &watchedDir{}, w.fw.Add(dir)
	}
	entries, err := os.ReadDir(dir)
	if err != nil {
		return nil, err
	}
	cost := len(entries) + 1
	if len(entries) > w.poll.threshold || w.cost+cost > w.poll.budget {
		return &watchedDir{polled: true, listing: readListing(dir, entries)}, nil
	}
	if err := w.fw.Add(dir); err != nil {
		return nil, err
	}
	w.cost += cost
	return &watchedDir{cost: cost}, nil
}

// trackCost counts the entries created and removed in a watched directory: fsnotify opens
// and closes a file for each of them. The directory is polled from the moment it has more
// entries than the threshold, or takes the cost of the watched directories over the budget.
func (w *Watcher) trackCost(name string, ev fsnotify.Event) {
	if w.poll.threshold <= 0 {
		return
	}
	dir := filepath.Dir(name)
	d := w.dirs[dir]
	if d == nil || d.polled {
		return
	}
	switch {
	case ev.Has(fsnotify.Create):
		d.cost++
		w.cost++
	case (ev.Has(fsnotify.Remove) || ev.Has(fsnotify.Rename)) && d.cost > 1:
		d.cost--
		w.cost--
	default:
		return
	}
	if entries := d.cost - 1; entries > w.poll.threshold || w.cost > w.poll.budget {
		w.startPolling(dir, d)
	}
}

// startPolling polls the watched directory instead of watching it with fsnotify.
func (w *Watcher) startPolling(dir string, d *watchedDir) {
	entries, err := os.ReadDir(dir)
	if err != nil {
		return // Removed: it's going to be forgotten.
	}
	_ = w.fw.Remove(dir)
	w.cost -= d.cost
	d.polled, d.cost, d.listing = true, 0, readListing(dir, entries)
}

// releaseDir stops watching or polling the directory.
func (w *Watcher) releaseDir(dir string) {
	d := w.dirs[dir]
	delete(w.dirs, dir)
	if !d.polled {
		_ = w.fw.Remove(dir) // It may be removed by fsnotify already.
		w.cost -= d.cost
	}
}

// removeDirs stops watching the directories of the subscription that no other one needs.
func (w *Watcher) removeDirs(s *subscription) {
	for dir := range s.dirs {
		if d := w.dirs[dir]; d != nil {
			if d.subs--; d.subs <= 0 {
				w.releaseDir(dir)
			}
		}
	}
	clear(s.dirs)
}

// forgetDir drops the removed or renamed directory and the ones inside it.
func (w *Watcher) forgetDir(dir string) {
	if _, watched := w.dirs[dir]; !watched {
		return
	}
	for d := range w.dirs {
		if d == dir || isInside(dir, d) {
			w.releaseDir(d)
			for _, s := range w.subs {
				delete(s.dirs, d)
			}
		}
	}
}

// pollDirs lists the polled directories and handles their changes since the last listing
// as the events of fsnotify.
func (w *Watcher) pollDirs() {
	w.mu.Lock()
	var dirs []string
	for dir, d := range w.dirs {
		if d.polled {
			dirs = append(dirs, dir)
		}
	}
	w.mu.Unlock()

	var events []fsnotify.Event
	for _, dir := range dirs {
		entries, err := os.ReadDir(dir)
		if errors.Is(err, fs.ErrNotExist) {
			events = append(events, fsnotify.Event{Name: dir, Op: fsnotify.Remove})
			continue
		} else if err != nil {
			continue // Try again next time.
		}
		listing := readListing(dir, entries)

		w.mu.Lock()
		if d := w.dirs[dir]; d != nil && d.polled { // It may be released in the meantime.
			events = append(events, listingChanges(dir, d.listing, listing)...)
			d.listing = listing
		}
		w.mu.Unlock()
	}
	for _, ev := range events {
		w.handle(ev)
	}
}

func readListing(dir string, entries []fs.DirEntry) map[string]entry {
	listing := make(map[string]entry, len(entries))
	for _, e := range entries {
		if e.IsDir() {
			listing[e.Name()] = entry{dir: true}
			continue
		}
		info, err := e.Info()
		if err != nil {
			continue // Removed already.
		}
		listing[e.Name()] = entry{size: info.Size(), modTime: info.ModTime()}
	}
	return listing
}

// listingChanges returns the events for the differences between the listings of the directory.
func listingChanges(dir string, prev, cur map[string]entry) []fsnotify.Event {
	var events []fsnotify.Event
	add := func(name string, op fsnotify.Op) {
		events = append(events, fsnotify.Event{Name: filepath.Join(dir, name), Op: op})
	}
	for name, e := range cur {
		p, existed := prev[name]
		switch {
		case !existed:
			add(name, fsnotify.Create)
		case p.dir != e.dir:
			add(name, fsnotify.Remove)
			add(name, fsnotify.Create)
		case !e.dir && (p.size != e.size || !p.modTime.Equal(e.modTime)):
			add(name, fsnotify.Write)
		}
	}
	for name := range prev {
		if _, exists := cur[name]; !exists {
			add(name, fsnotify.Remove)
		}
	}
	slices.SortFunc(events, func(a, b fsnotify.Event) int { return strings.Compare(a.Name, b.Name) })
	return events
}

func (w *Watcher) run() {
	timer := time.NewTimer(time.Hour)
	timer.Stop()
	var tick <-chan time.Time
	if w.poll.threshold > 0 {
		ticker := time.NewTicker(w.poll.interval)
		defer ticker.Stop()
		tick = ticker.C
	}
	for {
		select {
		case ev, ok := <-w.fw.Events:
			if !ok {
				return
			}
			w.handle(ev)
		case err, ok := <-w.fw.Errors:
			if !ok {
				return
			}
			if w.onError != nil {
				w.onError(err)
			}
		case <-timer.C:
		case <-tick:
			w.pollDirs()
		}

		calls, next := w.takeDue(time.Now())
		for _, call := range calls {
			call()
		}
		if !next.IsZero() {
			timer.Reset(max(time.Until(next), 0))
		}
	}
}

func (w *Watcher) handle(ev fsnotify.Event) {
	name := filepath.Clean(ev.Name)
	now := time.Now()

	w.mu.Lock()
	defer w.mu.Unlock()

	if ev.Has(fsnotify.Remove) || ev.Has(fsnotify.Rename) {
		w.forgetDir(name)
	}
	w.trackCost(name, ev)
	for _, s := range w.subs {
		if !s.matches(name, ev) {
			continue
		}
		if !slices.Contains(s.pending, name) {
			s.pending = append(s.pending, name)
		}
		s.due = now.Add(s.delay)

		if s.tree && ev.Has(fsnotify.Create) && filterEntry(filepath.Base(name)) {
			if fi, err := os.Stat(name); err == nil && fi.IsDir() {
				for _, d := range collectWatchPaths(name) {
					_ = w.addDir(s, d) // It may be gone already.
				}
			}
		}
	}
}

func (s *subscription) matches(name string, ev fsnotify.Event) bool {
	if s.dir {
		return filepath.Dir(name) == s.path &&
			(ev.Has(fsnotify.Create) || ev.Has(fsnotify.Remove) || ev.Has(fsnotify.Rename))
	}
	if !s.tree {
		return name == s.path && (ev.Has(fsnotify.Write) || ev.Has(fsnotify.Create))
	}
	// Only the directories of the tree: a directory inside it may be watched
	// for another subscription, like a hidden one.
	return s.dirs[filepath.Dir(name)] &&
		(ev.Has(fsnotify.Write) || ev.Has(fsnotify.Create) || ev.Has(fsnotify.Remove) || ev.Has(fsnotify.Rename))
}

// takeDue returns the callbacks of the subscriptions whose delay has passed,
// and the time the next one is due.
func (w *Watcher) takeDue(now time.Time) (calls []func(), next time.Time) {
	w.mu.Lock()
	defer w.mu.Unlock()
	for _, s := range w.subs {
		if len(s.pending) == 0 {
			continue
		}
		if !s.due.After(now) {
			paths, changed := s.pending, s.changed
			s.pending = nil
			calls = append(calls, func() { changed(paths) })
		} else if next.IsZero() || s.due.Before(next) {
			next = s.due
		}
	}
	return calls, next
}

func isInside(dir, path string) bool {
	return strings.HasPrefix(path, dir+string(filepath.Separator))
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
