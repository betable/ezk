package ezk

import (
	"fmt"
	"github.com/betable/retry"
	"github.com/samuel/go-zookeeper/zk"
	"time"
)

// Client is a wrapper over github.com/samuel/go-zookeeper/zk that retries some operations.
type Client struct {

	// The configuration for the client.
	Cfg ClientConfig

	// The underlying github.com/samuel/go-zookeeper/zk connection.
	Conn *zk.Conn

	// WatchCh will be nil until Ready is indicated
	// by being closed.
	WatchCh <-chan zk.Event

	// Ready is closed when the Zookeeper connection
	// is first made. After that you WatchCh will
	// no longer be nil and Watch events may
	// be received.
	Ready chan bool
}

// ClientConfig is used to configure a Client; pass
// it to NewClient().
type ClientConfig struct {
	Chroot         string
	Acl            []zk.ACL
	Servers        []string
	SessionTimeout time.Duration
	Retry          Retry
}

// NewClient creates a new ezk.Client.
// If the cfg.SessionTimout is set to 0
// a default value of 10 seconds will be used.
// If cfg.Retry is nil then the zk.defaultRetry
// function will be used.
func NewClient(cfg ClientConfig) *Client {
	if cfg.Retry == nil {
		fmt.Printf("cfg.Retry was nil, setting DefaultRetry\n")
		cfg.Retry = DefaultRetry
	}
	if cfg.SessionTimeout == 0 {
		cfg.SessionTimeout = 10 * time.Second
	}
	cli := &Client{
		Cfg:   cfg,
		Ready: make(chan bool),
	}
	return cli
}

// Retry defines the type of the retry method to use.
type Retry func(op, path string, f func() error)

// Connect connects to a Zookeeper server.
//
// Options can be passed to set a chroot or a default ACL.
func (z *Client) Connect() error {
	conn, ch, err := zk.Connect(z.Cfg.Servers, z.Cfg.SessionTimeout)
	if err != nil {
		return err
	}
	z.Conn = conn
	z.WatchCh = ch
	close(z.Ready)
	return nil
}

// Close closes the connection to the Zookeeper server.
func (z *Client) Close() {
	z.Conn.Close()
}

// Exists checks if a znode exists
func (z *Client) Exists(path string) (ok bool, s *zk.Stat, err error) {
	path = z.fullpath(path)
	z.Cfg.Retry("exists", path, func() error {
		ok, s, err = z.Conn.Exists(path)
		return err
	})
	return ok, s, err
}

// ExistsW returns if a znode exists and sets a watch.
func (z *Client) ExistsW(path string) (ok bool, s *zk.Stat, ch <-chan zk.Event, err error) {
	path = z.fullpath(path)
	z.Cfg.Retry("existsw", path, func() error {
		ok, s, ch, err = z.Conn.ExistsW(path)
		return err
	})
	return ok, s, ch, err
}

// Create creates a znode with a content.
func (z *Client) Create(path string, data []byte, flags int32, acl []zk.ACL) (s string, err error) {
	path = z.fullpath(path)
	if len(acl) == 0 && len(z.Cfg.Acl) != 0 {
		acl = z.Cfg.Acl
	}
	z.Cfg.Retry("create", path, func() error {
		s, err = z.Conn.Create(path, data, flags, acl)
		return err
	})
	return s, err
}

// Delete deletes a znode.
func (z *Client) Delete(path string, version int32) (err error) {
	path = z.fullpath(path)
	z.Cfg.Retry("delete", path, func() error {
		err = z.Conn.Delete(path, version)
		return err
	})
	return err
}

// Get returns the contents of a znode
func (z *Client) Get(path string) (d []byte, s *zk.Stat, err error) {
	path = z.fullpath(path)
	z.Cfg.Retry("get", path, func() error {
		d, s, err = z.Conn.Get(path)
		return err
	})
	return d, s, err
}

// GetW returns the contents of a znode and sets a watch
func (z *Client) GetW(path string) (d []byte, s *zk.Stat, ch <-chan zk.Event, err error) {
	path = z.fullpath(path)
	z.Cfg.Retry("getw", path, func() error {
		d, s, ch, err = z.Conn.GetW(path)
		return err
	})
	return d, s, ch, err
}

// Set writes content in an existent znode.
func (z *Client) Set(path string, data []byte, version int32) (s *zk.Stat, err error) {
	path = z.fullpath(path)
	z.Cfg.Retry("set", path, func() error {
		s, err = z.Conn.Set(path, data, version)
		return err
	})
	return s, err
}

// Children returns the children of a znode.
func (z *Client) Children(path string) (c []string, s *zk.Stat, err error) {
	path = z.fullpath(path)
	z.Cfg.Retry("children", path, func() error {
		c, s, err = z.Conn.Children(path)
		return err
	})
	return c, s, err
}

// ChildrenW returns the children of a znode and sets a watch.
func (z *Client) ChildrenW(path string) (c []string, s *zk.Stat, ch <-chan zk.Event, err error) {
	path = z.fullpath(path)
	z.Cfg.Retry("childrenw", path, func() error {
		c, s, ch, err = z.Conn.ChildrenW(path)
		return err
	})
	return c, s, ch, err
}

// Sync performs a sync from the master in the Zookeeper server.
func (z *Client) Sync(path string) (s string, err error) {
	path = z.fullpath(path)
	z.Cfg.Retry("sync", path, func() error {
		s, err = z.Conn.Sync(path)
		return err
	})
	return s, err
}

// GetACL returns the ACL for a znode.
func (z *Client) GetACL(path string) (a []zk.ACL, s *zk.Stat, err error) {
	path = z.fullpath(path)
	z.Cfg.Retry("getacl", path, func() error {
		a, s, err = z.Conn.GetACL(path)
		return err
	})
	return a, s, err
}

// SetACL sets a ACL to a znode.
func (z *Client) SetACL(path string, acl []zk.ACL, version int32) (s *zk.Stat, err error) {
	path = z.fullpath(path)
	z.Cfg.Retry("setacl", path, func() error {
		s, err = z.Conn.SetACL(path, acl, version)
		return err
	})
	return s, err
}

// CreateProtectedEphemeralSequential creates a sequential ephemeral znode.
func (z *Client) CreateProtectedEphemeralSequential(path string, data []byte, acl []zk.ACL) (string, error) {
	path = z.fullpath(path)
	return z.Conn.CreateProtectedEphemeralSequential(path, data, acl)
}

// CreateDir is a helper method that creates and empty znode if it does not exists.
func (z *Client) CreateDir(path string, acl []zk.ACL) error {
	path = z.fullpath(path)
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
	path = z.fullpath(path)
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
	path = z.fullpath(path)
	_, err := z.Sync(path)
	if err != nil {
		return nil, nil, err
	}

	return z.Get(path)
}

// fullpath returns the path with the chroot prepended. It can cause issues
// if the znode path starts like /foo/foo/...
func (z *Client) fullpath(path string) string {
	//	if z.Cfg.Chroot != "" && !strings.HasPrefix(path, z.Cfg.Chroot) {
	//    return z.Cfg.Chroot + path
	//	}
	//	return path

	return z.Cfg.Chroot + path
}

// The DefaultRetry function will retry four
// times if the first Zookeeper call fails, after sleeping
//  0ms, 100ms, 500ms and 1500ms.
func DefaultRetry(op, path string, f func() error) {
	retry.NewExecutor().
		WithRetries(4).
		WithBackoff(retry.ExponentialDelayBackoff(100*time.Millisecond, 5)).
		WithErrorComparator(func(err error) bool {
		return err == zk.ErrConnectionClosed || err == zk.ErrSessionExpired || err == zk.ErrSessionMoved
	}).Execute(f)
}
