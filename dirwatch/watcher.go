package dirwatch

import (
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
type Watcher struct {
	fw      *fsnotify.Watcher
	onError func(error)

	mu   sync.Mutex
	dirs map[string]int // watched directories with the number of subscriptions using them
	subs []*subscription
}

type subscription struct {
	path    string // the file or the root of the tree
	tree    bool
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
	fw, err := fsnotify.NewWatcher()
	if err != nil {
		return nil, err
	}
	w := &Watcher{
		fw:      fw,
		onError: onError,
		dirs:    make(map[string]int),
	}
	go w.run()
	return w, nil
}

// Close stops watching. The callback that is being called may still complete.
func (w *Watcher) Close() error {
	return w.fw.Close()
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
	if w.dirs[dir] == 0 {
		if err := w.fw.Add(dir); err != nil {
			return err
		}
	}
	w.dirs[dir]++
	s.dirs[dir] = true
	return nil
}

// removeDirs stops watching the directories of the subscription that no other one needs.
func (w *Watcher) removeDirs(s *subscription) {
	for dir := range s.dirs {
		w.dirs[dir]--
		if w.dirs[dir] <= 0 {
			delete(w.dirs, dir)
			_ = w.fw.Remove(dir)
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
			delete(w.dirs, d)
			_ = w.fw.Remove(d) // It may be removed by fsnotify already.
			for _, s := range w.subs {
				delete(s.dirs, d)
			}
		}
	}
}

func (w *Watcher) run() {
	timer := time.NewTimer(time.Hour)
	timer.Stop()
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
