package ezk

import (
	cv "github.com/glycerine/goconvey/convey"
	zook "github.com/samuel/go-zookeeper/zk"
	"testing"
	"time"
)

func Test001ClientRetryGetsDefault(t *testing.T) {
	cv.Convey("Given that we don't configure a Retry function, we should get the DefaultRetry function in our ClientConfig", t, func() {

		cli := NewClient(ClientConfig{})
		cv.So(cli.Cfg.Retry, cv.ShouldEqual, DefaultRetry)

	})
}

// In the Connect example, the goal is to set
// the value newURL into the node /chroot/service-name/config/server-url-list
func ExampleConnect() {
	newURL := "http://my-new-url.org:343/hello/enhanced-zookeeper-client"

	base := "/chroot"
	nsTest := "/service-name"
	subdir := "/config"
	path := nsTest + subdir + "/server-url-list"
	zkCfg := ClientConfig{
		Servers:        []string{"127.0.0.1:2181"},
		Acl:            zook.WorldACL(zook.PermAll),
		Chroot:         base,
		SessionTimeout: 10 * time.Second,
	}
	zk := NewClient(zkCfg)
	err := zk.Connect()
	if err != nil {
		panic(err)
	}

	defer zk.Close()

	zk.CreateNode(nsTest)
	zk.CreateNode(nsTest + subdir)
	zk.CreateNode(path)

	err = zk.DeleteNode(path) // delete any old value
	if err != nil {
		panic(err)
	}

	err = zk.CreateNode(path)
	if err != nil {
		panic(err)
	}

	_, err = zk.Set(path, []byte(newURL), -1)
	if err != nil {
		panic(err)
	}
}
