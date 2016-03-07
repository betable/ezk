package recipes

import (
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/betable/ezk"
	"github.com/samuel/go-zookeeper/zk"
)

func join(parts ...string) string {
	return strings.Join(parts, "/")
}

func parseSeq(path string) (int, error) {
	parts := strings.Split(path, ".")
	return strconv.Atoi(parts[len(parts)-1])
}

func unixMilli() int64 {
	return time.Now().UnixNano() / 1e6
}

// createSequentialLock creates a new lock and returns the full path of the lock.
func createSequentialLock(client *ezk.Client, base string, acl []zk.ACL) (string, error) {
	lockPath := fmt.Sprintf("%s/lock.", base)

	// Create lock attempt /base/_c_67c79fecc6104a7026bdd7c3ce773828-lock.0000000001
	return client.CreateProtectedEphemeralSequential(lockPath, nil, acl)
}

// sequentialLockFight creates a new lock and does a sequentialFight.
// It returns the new lock and the result of the sequential fight.
func sequentialLockFight(client *ezk.Client, base string, acl []zk.ACL) (string, error) {
	// Create lock
	lock, err := createSequentialLock(client, base, acl)
	if err != nil {
		return "", err
	}

	// Grab the sequence number
	seq, err := parseSeq(lock)
	if err != nil {
		return lock, err
	}

	// Fight and return result
	_, err = sequentialFight(client, base, seq)

	return lock, err
}

// sequentialFight returns the path to the lock with the lowest sequence number.
// If that file is the same the one passed it returns nil meaning that that sequence
// number has win the fight, if not it will return ErrLockFound.
func sequentialFight(client *ezk.Client, base string, seq int) (string, error) {
	// Read all the childrens
	children, _, err := client.Children(base)
	if err != nil {
		return "", err
	}

	// Look for the lowest sequence number
	var lowestNode string
	lowestSeq := seq
	for _, p := range children {
		s, err := parseSeq(p)
		// ignore unknown znodes
		if err == nil {
			if s < lowestSeq {
				lowestSeq = s
				lowestNode = p
			}
		}
	}

	// Add the base path
	lowestNode = join(base, lowestNode)

	// Acquire the lock
	if seq == lowestSeq {
		return lowestNode, nil
	}

	return lowestNode, ErrLockFound
}

// timeBasedCleaner deletes those znodes in the base path older than t.
func timeBasedCleaner(client *ezk.Client, base string, t time.Duration) error {
	now := unixMilli()

	// Read all the childrens
	children, _, err := client.Children(base)
	if err != nil {
		return err
	}

	// Iterate to all the childrents reading the creation time
	millis := int64(t / time.Millisecond)
	for i := range children {
		node := join(base, children[i])
		ok, stat, err := client.Exists(node)
		if err != nil {
			return err
		}

		// Delete locks older than SchedulerLockTime
		if ok && stat.Ctime+millis < now {
			if err := client.Delete(node, stat.Version); err != nil && err != zk.ErrNoNode {
				return err
			}
		}
	}

	return nil
}
