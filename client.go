package ezk

import (
	"strings"
	"time"

	"github.com/betable/retry"
	"github.com/samuel/go-zookeeper/zk"
)

// Client is a wrapper over github.com/samuel/go-zookeeper/zk that retries some operations.
type Client struct {
	conn   *zk.Conn
	chroot string
	acl    []zk.ACL
	retry  Retry
}

// Retry defines the type of the retry method to use.
type Retry func(op, path string, f func() error)

type option func(*Client)

// Connect connects to a Zookeeper server. If the sessionTimout is set to 0
// a default value of 10 seconds will be used.
//
// Options can be passed to set a chroot or a default ACL.
func Connect(servers []string, sessionTimeout time.Duration, opts ...option) (*Client, <-chan zk.Event, error) {
	if sessionTimeout == 0 {
		sessionTimeout = 10 * time.Second
	}
	conn, ch, err := zk.Connect(servers, sessionTimeout)
	if err != nil {
		return nil, nil, err
	}

	client := &Client{
		conn:  conn,
		retry: defaultRetry,
	}

	client.Options(opts...)

	return client, ch, nil
}

// ChRoot defines a path that will be prepended to all the operations.
func ChRoot(root string) option {
	return func(z *Client) {
		z.chroot = root
	}
}

// DefaultACL defines an ACL that will be used if none is provided.
func DefaultACL(acl []zk.ACL) option {
	return func(z *Client) {
		z.acl = acl
	}
}

// SetRetryFunc defines a custom retry function.
func SetRetryFunc(f Retry) option {
	return func(z *Client) {
		z.retry = f
	}
}

// Options sets the options in the client.
func (z *Client) Options(opts ...option) {
	for _, opt := range opts {
		opt(z)
	}
}

// Close closes the connection to the Zookeeper server.
func (z *Client) Close() {
	z.conn.Close()
}

// Exists returns if a znode exists
func (z *Client) Exists(path string) (ok bool, s *zk.Stat, err error) {
	path = z.fullpath(path)
	z.retry("exists", path, func() error {
		ok, s, err = z.conn.Exists(path)
		return err
	})
	return ok, s, err
}

// ExistsW returns if a znode exists and sets a watch.
func (z *Client) ExistsW(path string) (ok bool, s *zk.Stat, ch <-chan zk.Event, err error) {
	path = z.fullpath(path)
	z.retry("existsw", path, func() error {
		ok, s, ch, err = z.conn.ExistsW(path)
		return err
	})
	return ok, s, ch, err
}

// Create creates a znode with a content.
func (z *Client) Create(path string, data []byte, flags int32, acl []zk.ACL) (s string, err error) {
	path = z.fullpath(path)
	if len(acl) == 0 && len(z.acl) != 0 {
		acl = z.acl
	}
	z.retry("create", path, func() error {
		s, err = z.conn.Create(path, data, flags, acl)
		return err
	})
	return s, err
}

// Delete deletes a znode.
func (z *Client) Delete(path string, version int32) (err error) {
	path = z.fullpath(path)
	z.retry("delete", path, func() error {
		err = z.conn.Delete(path, version)
		return err
	})
	return err
}

// Get returns the contents of a znode
func (z *Client) Get(path string) (d []byte, s *zk.Stat, err error) {
	path = z.fullpath(path)
	z.retry("get", path, func() error {
		d, s, err = z.conn.Get(path)
		return err
	})
	return d, s, err
}

// GetW returns the contents of a znode and sets a watch
func (z *Client) GetW(path string) (d []byte, s *zk.Stat, ch <-chan zk.Event, err error) {
	path = z.fullpath(path)
	z.retry("getw", path, func() error {
		d, s, ch, err = z.conn.GetW(path)
		return err
	})
	return d, s, ch, err
}

// Set writes content in an existent znode.
func (z *Client) Set(path string, data []byte, version int32) (s *zk.Stat, err error) {
	path = z.fullpath(path)
	z.retry("set", path, func() error {
		s, err = z.conn.Set(path, data, version)
		return err
	})
	return s, err
}

// Children returns the children of a znode.
func (z *Client) Children(path string) (c []string, s *zk.Stat, err error) {
	path = z.fullpath(path)
	z.retry("children", path, func() error {
		c, s, err = z.conn.Children(path)
		return err
	})
	return c, s, err
}

// ChildrenW returns the children of a znode and sets a watch.
func (z *Client) ChildrenW(path string) (c []string, s *zk.Stat, ch <-chan zk.Event, err error) {
	path = z.fullpath(path)
	z.retry("childrenw", path, func() error {
		c, s, ch, err = z.conn.ChildrenW(path)
		return err
	})
	return c, s, ch, err
}

// Sync performs a sync from the master in the Zookeeper server.
func (z *Client) Sync(path string) (s string, err error) {
	path = z.fullpath(path)
	z.retry("sync", path, func() error {
		s, err = z.conn.Sync(path)
		return err
	})
	return s, err
}

// GetACL returns the ACL for a znode.
func (z *Client) GetACL(path string) (a []zk.ACL, s *zk.Stat, err error) {
	path = z.fullpath(path)
	z.retry("getacl", path, func() error {
		a, s, err = z.conn.GetACL(path)
		return err
	})
	return a, s, err
}

// SetACL sets a ACL to a znode.
func (z *Client) SetACL(path string, acl []zk.ACL, version int32) (s *zk.Stat, err error) {
	path = z.fullpath(path)
	z.retry("setacl", path, func() error {
		s, err = z.conn.SetACL(path, acl, version)
		return err
	})
	return s, err
}

// CreateProtectedEphemeralSequential creates a sequential ephemeral znode.
func (z *Client) CreateProtectedEphemeralSequential(path string, data []byte, acl []zk.ACL) (string, error) {
	// Using the default retry mechanism
	path = z.fullpath(path)
	return z.conn.CreateProtectedEphemeralSequential(path, data, acl)
}

// CreateDir is a helper method that creates and empty znode if it does not exists.
func (z *Client) CreateDir(path string, acl []zk.ACL) error {
	ok, _, err := z.Exists(path)
	if err != nil {
		return err
	}

	if !ok {
		_, err = z.Create(path, []byte{}, 0, acl)
		if err == zk.ErrNodeExists {
			return nil
		}
	}

	return err
}

// SafeSet is a helper method that writes a znode creating it first if it does not exists.
func (z *Client) SafeSet(path string, data []byte, version int32, acl []zk.ACL) (*zk.Stat, error) {
	_, err := z.Sync(path)
	if err != nil {
		return nil, err
	}

	ok, _, err := z.Exists(path)
	if err != nil {
		return nil, err
	}

	if !ok {
		_, err := z.Create(path, data, 0, acl)
		if err != nil {
			return nil, err
		}
		_, s, err := z.Exists(path)
		return s, err
	}

	return z.Set(path, data, version)
}

// SafeGet is a helper method that syncs Zookeeper and return the content of a znode.
func (z *Client) SafeGet(path string) ([]byte, *zk.Stat, error) {
	_, err := z.Sync(path)
	if err != nil {
		return nil, nil, err
	}

	return z.Get(path)
}

// fullpath returns the path with the chroot prepended. It can cause issues
// if the znode path starts like /foo/foo/...
func (z *Client) fullpath(path string) string {
	if z.chroot != "" && !strings.HasPrefix(path, z.chroot) {
		return z.chroot + path
	}

	return path
}

func defaultRetry(op, path string, f func() error) {
	// If the function fails it will retry 4 extra times times
	// sleeping: 0ms, 100ms, 500ms and 1500ms
	retry.NewExecutor().
		WithRetries(4).
		WithBackoff(retry.ExponentialDelayBackoff(100*time.Millisecond, 5)).
		WithErrorComparator(func(err error) bool {
		return err == zk.ErrConnectionClosed || err == zk.ErrSessionExpired || err == zk.ErrSessionMoved
	}).Execute(f)
}
