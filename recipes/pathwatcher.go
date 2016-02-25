package recipes

import (
	"sync"

	"github.com/betable/ezk"
	"github.com/samuel/go-zookeeper/zk"
)

// PathWatcher is a recipe that watches for child events in a ZNode.
// It will trigger an event when a child is created or deleted.
//
// Usage:
// 	client := ezk.NewClient(ezk.ClientConfig{})
// 	watcher := recipes.NewPathWatcher(client, "/")
// 	watcher.Start()
//	go func() {
//		for {
// 			select {
// 				case e, ok := <-watcher.Event():
// 				if !ok {
// 					return
// 				}
// 				if e.Type == zk.EventNodeChildrenChanged {
// 					// do something
// 				}
//
// 				case err := <-monitorWatcher.Error():
// 					// do something
// 			}
// 		}
//	}()
//	...
//	// Stop the watcher
//	watcher.Stop()
type PathWatcher struct {
	Path   string
	client *ezk.Client
	mut    sync.Mutex
	event  chan zk.Event
	stop   chan struct{}
	done   chan struct{}
	err    chan error
}

// NewPathWatcher creates a new PathWatcher on the passed path.
func NewPathWatcher(client *ezk.Client, path string) *PathWatcher {
	return &PathWatcher{
		Path:   path,
		client: client,
		event:  make(chan zk.Event),
		err:    make(chan error),
		stop:   make(chan struct{}),
		done:   make(chan struct{}),
	}
}

// Event returns the channel used to listen for child events.
func (p *PathWatcher) Event() <-chan zk.Event {
	return p.event
}

// Error is the channel used to listen for errors.
func (p *PathWatcher) Error() <-chan error {
	return p.err
}

// Start starts PathWatcher. It will send the events to event channel and the
// errors to th error channel.
func (p *PathWatcher) Start() {
	go func() {
		for {
			_, _, ch, err := p.client.ChildrenW(p.Path)
			if err != nil {
				p.err <- err
				continue
			}

			select {
			case e := <-ch:
				p.event <- e
			case <-p.stop:
				close(p.event)
				close(p.done)
				return
			}
		}
	}()
}

// Stop stops the watcher. Stop will close the event channel and return once
// the watcher finishes.
func (p *PathWatcher) Stop() {
	p.mut.Lock()
	select {
	case <-p.stop:
	default:
		close(p.stop)
	}
	p.mut.Unlock()
	<-p.done
}
