/*
Package ezk is an enhanced Zookeeper client. It uses the
connection and underlying operations from
https://github.com/samuel/go-zookeeper/zk
in conjunction with automatic retry of operations via the
https://github.com/betable/retry library.

    CHROOT and ABSOLUTE vs RELATIVE paths
    =====================================

    Our (enforced) zookeeper path convention for the
    ClientConfig.Chroot string allows us to distinguish
    between chroot-ed paths and non-chroot paths as follows:

    * the Chroot prefix must always start with a forward slash.
    A single '/' alone is a valid Chroot prefix. If the prefix
    is longer than one character, the Chroot prefix must
    also end with a '/' as well--in this case the prefix will
    contain exactly two '/' slash characters; one at the
    beginning and one at the end.

    A Chroot prefix therefore names either zero or one
    znodes.

    For example: "/prod/", "/staging/", and "/devtest/" are all
    legal Chroot strings. Counter-examples: "/prod" is not
    a legal Chroot value, nor is "prod/", nor is "prod".
    User code can distinguish Chroot prefixes by checking
    whether the first byte of the string is '/' or not.
    See the IsAbsolutePath() and RemoveChroot()
    helper functions.

    * All paths that are intended to be relative to the Chroot
    prefix must *not* start with a forward slash and must
    not end with a forward slash; they are like relative paths in Unix.

    So legal examples of relative paths: "myservice/config/my-servers",
    "piper", or "timeseries", or "timeseris/config". The
    requirement forbidding the trailing '/' is enforced on the
    Zookeeper server side.

    * The ezk library will form the full path (spoken on the wire to
    the Zookeeper) by a simple concatenation of Chroot + relative path.

    * The helper function RemoveChroot(path) will detect and
    automatically remove any chroot prefix from path, and returns a
    relative path. It will leave untouched already relative paths.

    * Important: when receiving watch events on channels from the
    github.com/samuel/go-zookeeper/zk library, they are of type

    type Event struct {
        Type   EventType
        State  State
        Path   string // For non-session events, the (absolute, Chroot-prefixed) path of the watched node. [1]
        Err    error
        Server string // For connection events
    }

    Note [1] that this Path is absolute, it includes the Chroot
    prefix. Users should call RemoveChroot() function as needed
    before using the Event.Path field if they require a relative
    path.
*/
package ezk

import (
	"fmt"
	"github.com/betable/retry"
	"github.com/samuel/go-zookeeper/zk"
	"strings"
	"time"
)

// Client is a wrapper over github.com/samuel/go-zookeeper/zk that retries all but one of its operations according to the ClientConfig.Retry function. The one exception is for CreateProtectedEphemeralSequential(); it is not retried automatically.
type Client struct {

	// The configuration for the client.
	Cfg ClientConfig

	// The underlying github.com/samuel/go-zookeeper/zk connection.
	Conn *zk.Conn

	// WatchCh will be nil until Connect returns without error.
	// Watches that fire over the Zookeeper connection will be
	// received on WatchCh.
	WatchCh <-chan zk.Event
}

// ClientConfig is used to configure a Client; pass
// it to NewClient().
type ClientConfig struct {

	// The Chroot directory will be prepended to all paths
	Chroot string

	// The set of ACLs used by defeault when
	// calling Client.Create(), if the formal
	// parameter acl in Create() is length 0.
	Acl []zk.ACL

	// The URLs of the zookeepers to attempt to connect to.
	Servers []string

	// SessionTimeout defaults to 10 seconds if not
	// otherwise set.
	SessionTimeout time.Duration

	// The Retry function determines how many times
	// and how often we retry our Zookeeper operations
	// before failing. See DefaultRetry() which is
	// used if this is not otherwise set.
	Retry Retry
}

// NewClient creates a new ezk.Client.
// If the cfg.SessionTimout is set to 0
// a default value of 10 seconds will be used.
// If cfg.Retry is nil then the DefaultRetry
// function will be used.
// If cfg.Acl is length 0 then zk.WorldACL(zk.PermAll)
// will be used.
func NewClient(cfg ClientConfig) *Client {

	// handle "" or "/prod" Chroot gracefully.
	if !strings.HasSuffix(cfg.Chroot, "/") {
		cfg.Chroot += "/"
	}

	if cfg.Retry == nil {
		cfg.Retry = DefaultRetry
	}
	if cfg.SessionTimeout == 0 {
		cfg.SessionTimeout = 10 * time.Second
	}
	if len(cfg.Acl) == 0 {
		cfg.Acl = zk.WorldACL(zk.PermAll)
	}
	cli := &Client{
		Cfg: cfg,
	}
	return cli
}

// Retry defines the type of the retry method to use.
type Retry func(op, path string, f func() error)

// Connect connects to a Zookeeper server.
// Upon success it sets the z.WatchCh and
// z.Conn and returns nil.
// If an error is returned then z.WatchCh and
// z.Conn will not be set. Connect() will
// typically only be needed once, but can be
// called again if need be.
// Upon successful connection to Zookeeper,
// we will attempt to create the z.Cfg.Chroot node.
// No error will be returned if this attempt
// fails, as commonly it may already exist.
func (z *Client) Connect() error {
	z.WatchCh = nil
	z.Conn = nil
	conn, ch, err := zk.Connect(z.Cfg.Servers, z.Cfg.SessionTimeout)
	if err != nil {
		return err
	}
	z.Conn = conn
	z.WatchCh = ch

	// make the Chroot dir; deliberately ignore err as
	// the Chroot node may already exist.
	z.Create("", []byte{}, 0, z.Cfg.Acl)
	return nil
}

// Close closes the connection to the Zookeeper server.
func (z *Client) Close() {
	z.Conn.Close()
}

// Exists checks if a znode exists.
// z.Cfg.Chroot will be prepended to a relative path. The call will be retried.
func (z *Client) Exists(path string) (ok bool, s *zk.Stat, err error) {
	path = z.fullpath(path)
	z.Cfg.Retry("exists", path, func() error {
		ok, s, err = z.Conn.Exists(path)
		return err
	})
	return ok, s, err
}

// ExistsW returns if a znode exists and sets a watch.
// z.Cfg.Chroot will be prepended to a relative path. The call will be retried.
func (z *Client) ExistsW(path string) (ok bool, s *zk.Stat, ch <-chan zk.Event, err error) {
	path = z.fullpath(path)
	z.Cfg.Retry("existsw", path, func() error {
		ok, s, ch, err = z.Conn.ExistsW(path)
		return err
	})
	return ok, s, ch, err
}

// Create creates a znode with a content. If
// acl is nil then the z.Cfg.Acl set will be
// applied to the new znode.
// z.Cfg.Chroot will be prepended to a relative path. The call will be retried.
func (z *Client) Create(path string, data []byte, flags int32, acl []zk.ACL) (s string, err error) {
	path = z.fullpath(path)
	if len(acl) == 0 && len(z.Cfg.Acl) != 0 {
		acl = z.Cfg.Acl
	}
	z.Cfg.Retry("create", path, func() error {
		q("Create on path='%s'\n", path)
		s, err = z.Conn.Create(path, data, flags, acl)
		return err
	})
	return s, err
}

// Delete deletes a znode.
// z.Cfg.Chroot will be prepended to a relative path. The call will be retried.
func (z *Client) Delete(path string, version int32) (err error) {
	path = z.fullpath(path)
	z.Cfg.Retry("delete", path, func() error {
		err = z.Conn.Delete(path, version)
		return err
	})
	return err
}

// Get returns the contents of a znode.
// z.Cfg.Chroot will be prepended to a relative path. The call will be retried.
func (z *Client) Get(path string) (d []byte, s *zk.Stat, err error) {
	path = z.fullpath(path)
	z.Cfg.Retry("get", path, func() error {
		q("Get on path='%s'\n", path)
		d, s, err = z.Conn.Get(path)
		return err
	})
	return d, s, err
}

// GetW returns the contents of a znode and sets a watch.
// z.Cfg.Chroot will be prepended to a relative path. The call will be retried.
func (z *Client) GetW(path string) (d []byte, s *zk.Stat, ch <-chan zk.Event, err error) {
	path = z.fullpath(path)
	z.Cfg.Retry("getw", path, func() error {
		q("GetW on path='%s'\n", path)
		d, s, ch, err = z.Conn.GetW(path)
		return err
	})
	return d, s, ch, err
}

// Set writes content in an existent znode.
// z.Cfg.Chroot will be prepended to a relative path. The call will be retried.
func (z *Client) Set(path string, data []byte, version int32) (s *zk.Stat, err error) {
	path = z.fullpath(path)
	z.Cfg.Retry("set", path, func() error {
		q("Set on path='%s'\n", path)
		s, err = z.Conn.Set(path, data, version)
		return err
	})
	return s, err
}

// Children returns the children of a znode.
// z.Cfg.Chroot will be prepended to a relative path. The call will be retried.
func (z *Client) Children(path string) (c []string, s *zk.Stat, err error) {
	path = z.fullpath(path)
	z.Cfg.Retry("children", path, func() error {
		c, s, err = z.Conn.Children(path)
		return err
	})
	return c, s, err
}

// ChildrenW returns the children of a znode and sets a watch.
// z.Cfg.Chroot will be prepended to a relative path. The call will be retried.
func (z *Client) ChildrenW(path string) (c []string, s *zk.Stat, ch <-chan zk.Event, err error) {
	path = z.fullpath(path)
	z.Cfg.Retry("childrenw", path, func() error {
		c, s, ch, err = z.Conn.ChildrenW(path)
		return err
	})
	return c, s, ch, err
}

// Sync performs a sync from the master in the Zookeeper server.
// z.Cfg.Chroot will be prepended to a relative path. The call will be retried.
func (z *Client) Sync(path string) (s string, err error) {
	path = z.fullpath(path)
	z.Cfg.Retry("sync", path, func() error {
		s, err = z.Conn.Sync(path)
		return err
	})
	return s, err
}

// GetACL returns the ACL for a znode.
// z.Cfg.Chroot will be prepended to a relative path. The call will be retried.
func (z *Client) GetACL(path string) (a []zk.ACL, s *zk.Stat, err error) {
	path = z.fullpath(path)
	z.Cfg.Retry("getacl", path, func() error {
		a, s, err = z.Conn.GetACL(path)
		return err
	})
	return a, s, err
}

// SetACL sets a ACL to a znode.
// z.Cfg.Chroot will be prepended to a relative path. The call will be retried.
func (z *Client) SetACL(path string, acl []zk.ACL, version int32) (s *zk.Stat, err error) {
	path = z.fullpath(path)
	z.Cfg.Retry("setacl", path, func() error {
		s, err = z.Conn.SetACL(path, acl, version)
		return err
	})
	return s, err
}

// CreateProtectedEphemeralSequential creates a sequential ephemeral znode.
// z.Cfg.Chroot will be prepended to a relative path. The call will be NOT be retried.
func (z *Client) CreateProtectedEphemeralSequential(path string, data []byte, acl []zk.ACL) (string, error) {
	path = z.fullpath(path)
	return z.Conn.CreateProtectedEphemeralSequential(path, data, acl)
}

// CreateDir is a helper method that creates a path of empty
// znodes if they do not exist. It acts like os.MkdirAll or
// mkdir -p in that it will create a series of nested directories
// if the path supplies them. It won't fail if some or all of
// these already exist. z.Cfg.Chroot will be prepended to a
// relative path. The call will be retried.
func (z *Client) CreateDir(path string, acl []zk.ACL) error {
	path = z.fullpath(path)
	if path == "" || path == "/" {
		return nil
	}

	next := ""
	elem := strings.Split(ChompSlash(path[1:]), "/")
	if len(acl) == 0 {
		acl = z.Cfg.Acl
	}
	var err error
	for _, e := range elem {
		next += "/" + e
		_, err = z.Create(next, []byte{}, 0, acl)
		if err == zk.ErrNodeExists || err == nil {
			continue
		} else {
			return err
		}
	}
	return nil
}

// SafeSet is a helper method that writes a znode creating it first if it does not exists. It will sync the Zookeeper before checking if the node exists.
// z.Cfg.Chroot will be prepended to a relative path. The call will be retried.
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
// z.Cfg.Chroot will be prepended to a relative path. The call will be retried.
func (z *Client) SafeGet(path string) ([]byte, *zk.Stat, error) {
	_, err := z.Sync(path)
	if err != nil {
		return nil, nil, err
	}

	return z.Get(path)
}

// If path is absolute, fullpath leaves it unchanged. Otherwise,
// fullpath returns the path with the chroot prepended.
func (z *Client) fullpath(path string) string {
	if len(path) == 0 {
		return ChompSlash(z.Cfg.Chroot)
	}
	if IsAbsolutePath(path) {
		return ChompSlash(path)
	}
	return ChompSlash(z.Cfg.Chroot + path)
}

// The DefaultRetry function will retry four times if the
// first Zookeeper call fails, after sleeping in turn: 0ms, 100ms, 500ms and 1500ms.
func DefaultRetry(op, path string, f func() error) {
	retry.NewExecutor().
		WithRetries(4).
		WithBackoff(retry.ExponentialDelayBackoff(100*time.Millisecond, 5)).
		WithErrorComparator(func(err error) bool {
		return err == zk.ErrConnectionClosed || err == zk.ErrSessionExpired || err == zk.ErrSessionMoved
	}).Execute(f)
}

// A convenience version of Delete, DeleteNode deletes a znode,
// using version -1 to delete any version.
// z.Cfg.Chroot will be prepended to a relative path. The call will be retried.
func (z *Client) DeleteNode(path string) error {
	return z.Delete(path, -1)
}

// A convenience version of Delete, DeleteNodeRecursively deletes a znode
// and any of its children, using version -1 to delete any version.
// z.Cfg.Chroot will be prepended to a relative path. The call will be retried.
func (z *Client) DeleteNodeRecursively(path string) error {

	// find children and delete then
	chldList, _, err := z.Children(path)
	switch err {
	case nil:
	case zk.ErrNoNode:
		// no children, ignore
	default:
		return err
	}
	for _, chld := range chldList {
		err = z.DeleteNodeRecursively(path + "/" + chld)
		if err != nil {
			return err
		}
	}
	return z.Delete(path, -1)
}

// A convenience version of Create, CreateNode supplies
// empty data, 0 flags, and the default z.Cfg.Acl list
// to a z.Create() call.
// z.Cfg.Chroot will be prepended to a relative path. The call will be retried.
func (z *Client) CreateNode(path string) error {
	_, err := z.Create(path, []byte{}, 0, z.Cfg.Acl)
	return err
}

// RemoveChroot(path) will detect and
// automatically remove any chroot prefix, and return a
// relative path.
//
// Examples: "/mybase/myservice/config" -> "myservice/config"
//           "/myroot/alist" -> "alist"
//           "/hello/" -> ""
//           "/poorlyFormed" -> ""  ## properly should have a trailling slash
//           "relative/path/unchanged" -> "relative/path/unchanged"
//
// RemoveChroot will leave untouched any already
// relative paths. Note that this returned relative path
// may be the empty string "" if path consists of one element
// "/chroot/" alone, for example.
func RemoveChroot(path string) string {
	if !IsAbsolutePath(path) {
		return path
	}

	// find the second '/', but don't freak if its not there.
	n := strings.Index(path[1:], "/")
	if n == -1 {
		// not found
		return ""
	}
	return path[2+n:]
}

// IsAbsolutePath() checks to see if the path
// starts with a Chroot element by checking
// if the first byte is a '/' or not.
func IsAbsolutePath(path string) bool {
	if len(path) == 0 {
		return false
	}
	if path[0] == '/' {
		return true
	}
	return false
}

// ChompSlash removes any trailing single '/' from path. It
// only checks the very last character.
func ChompSlash(path string) string {
	if strings.HasSuffix(path, "/") {
		return path[:len(path)-1]
	}
	return path
}

// print with newlines around it; for debug tracing.
func p(format string, stuff ...interface{}) {
	fmt.Printf("\n "+format+"\n", stuff...)
}

// q calls are quietly ignored. They allow conversion from p()
// calls easily.
func q(quietly_ignored ...interface{}) {} // quiet
