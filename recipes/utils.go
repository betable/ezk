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
	err = sequentialFight(client, base, seq)

	return lock, err
}

func createSequentialLock(client *ezk.Client, base string, acl []zk.ACL) (string, error) {
	lockPath := fmt.Sprintf("%s/lock.", base)

	// Create lock attempt /base/_c_67c79fecc6104a7026bdd7c3ce773828-lock.0000000001
	return client.CreateProtectedEphemeralSequential(lockPath, nil, acl)
}

func sequentialFight(client *ezk.Client, base string, seq int) error {
	// Read all the childrens
	children, _, err := client.Children(base)
	if err != nil {
		return err
	}

	// Look for the lowest sequence number
	lowestSeq := seq
	for _, p := range children {
		s, err := parseSeq(p)
		// ignore unknown znodes
		if err == nil {
			if s < lowestSeq {
				lowestSeq = s
			}
		}
	}

	// Acquire the lock
	if seq == lowestSeq {
		return nil
	}

	return ErrLockFound
}

func timeBasedCleaner(client *ezk.Client, base string, t time.Duration) error {
	now := unixMilli()

	// Read all the childrens
	children, _, err := client.Children(base)
	if err != nil {
		return err
	}

	// Iterate to all the childrents reading the creation time
	seconds := int64(t / time.Second)
	for i := range children {
		node := join(base, children[i])
		ok, stat, err := client.Exists(node)
		if err != nil {
			return err
		}
		println(ok, stat.Ctime, seconds, now, stat.Ctime+seconds < now)
		// Delete locks older than SchedulerLockTime
		if ok && stat.Ctime+seconds < now {
			if err := client.Delete(node, stat.Version); err != nil && err != zk.ErrNoNode {
				return err
			}
		}
	}

	return nil
}
