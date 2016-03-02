package recipes

import (
	"time"

	"github.com/betable/ezk"
	"github.com/samuel/go-zookeeper/zk"
)

type Lock struct {
	Path   string
	client *ezk.Client
	acl    []zk.ACL
	lock   string
}

// NewLock initializes a new distributed lock.
func NewLock(client *ezk.Client, path string, acl []zk.ACL) *Lock {
	return &Lock{
		Path:   path,
		client: client,
		acl:    acl,
	}
}

// Lock implements a distributed lock on zookeeper where only one instance will
// be able to process while the rest of them will wait.
func (l *Lock) Lock() error {
	var err error

	// Create lock
	l.lock, err = createSequentialLock(l.client, l.Path, l.acl)
	if err != nil {
		l.Unlock()
		return err
	}

	// Grab the sequence number
	seq, err := parseSeq(l.lock)
	if err != nil {
		l.Unlock()
		return err
	}

	// Fight and wait until it's free
	for {
		lockFile, err := sequentialFight(l.client, l.Path, seq)
		switch err {
		case nil:
			// lock acquired
			return nil
		case ErrLockFound:
			// Lock acquired by another user.
			// Create a exist watcher on the file that holds the lock so we
			// can wait until it gets deleted and then we fight again.
			ok, _, ch, err := l.client.ExistsW(lockFile)
			if err != nil {
				l.Unlock()
				return err
			}
			if ok {
				<-ch
			}
			continue
		default:
			// error reading the locks
			l.Unlock()
			return err
		}
	}

}

// LockWithCleaner implements a distributed lock but before locking it will
// clean the locks older than t.
func (l *Lock) LockWithCleaner(t time.Duration) error {
	// Clean old locks
	if err := timeBasedCleaner(l.client, l.Path, t); err != nil {
		return err
	}
	// Lock
	return l.Lock()
}

// TryLock will attempt to acquire the lock only if it is free at the time of invocation.
// If it's free it will return nil, if not it will return the error recipes.ErrLockFound
// unless any other error is found.
func (l *Lock) TryLock() error {
	var err error
	// Create lock and fight
	l.lock, err = sequentialLockFight(l.client, l.Path, l.acl)
	if err != nil {
		l.Unlock()
		return err
	}
	return nil
}

// LockWithCleaner implements a distributed TryLock but before locking it will
// clean the locks older than t.
func (l *Lock) TryLockWithCleaner(t time.Duration) error {
	// Clean old locks
	if err := timeBasedCleaner(l.client, l.Path, t); err != nil {
		return err
	}
	// TryLock
	return l.TryLock()
}

// Unlock removes the current lock znode.
func (l *Lock) Unlock() error {
	if l.lock == "" {
		return nil
	}
	return l.client.Delete(l.lock, -1)
}

// CleanOlderLocks removes the locks in a path that are older than t.
func CleanOlderLocks(client *ezk.Client, base string, t time.Duration) error {
	return timeBasedCleaner(client, base, t)
}
