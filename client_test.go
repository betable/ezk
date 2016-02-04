package ezk

import (
	cv "github.com/glycerine/goconvey/convey"
	"testing"
)

func Test001ClientRetryGetsDefault(t *testing.T) {
	cv.Convey("Given that we don't configure a Retry function, we should get the DefaultRetry function in our ClientConfig", t, func() {

		cli := NewClient(ClientConfig{})
		cv.So(cli.Cfg.Retry, cv.ShouldEqual, DefaultRetry)

	})
}
