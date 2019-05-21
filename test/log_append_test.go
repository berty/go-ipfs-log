package test

import (
	"context"
	"fmt"
	"testing"
	"time"

	idp "github.com/berty/go-ipfs-log/identityprovider"
	io "github.com/berty/go-ipfs-log/io"
	ks "github.com/berty/go-ipfs-log/keystore"
	"github.com/berty/go-ipfs-log/log"
	ds "github.com/ipfs/go-datastore"
	dssync "github.com/ipfs/go-datastore/sync"

	. "github.com/smartystreets/goconvey/convey"
)

func TestLogAppend(t *testing.T) {
	_, cancel := context.WithTimeout(context.Background(), time.Second*10)
	defer cancel()

	ipfs := io.NewMemoryServices()

	datastore := dssync.MutexWrap(ds.NewMapDatastore())
	keystore, err := ks.NewKeystore(datastore)
	if err != nil {
		panic(err)
	}

	idProvider := idp.NewOrbitDBIdentityProvider(keystore)
	identity, err := idProvider.GetID("User1")
	if err != nil {
		panic(err)
	}

	Convey("Log - Append", t, FailureHalts, func(c C) {
		c.Convey("append", FailureHalts, func(c C) {
			c.Convey("append one", FailureHalts, func(c C) {
				log1 := log.NewLog(ipfs, identity, &log.NewLogOptions{ID: "A"})
				_, err := log1.Append([]byte("hello1"), 1)
				c.So(err, ShouldBeNil)

				c.So(len(log1.Entries), ShouldEqual, 1)
				for _, v := range log1.Values() {
					c.So(string(v.Payload), ShouldEqual, "hello1")
					c.So(len(v.Next), ShouldEqual, 0)
					c.So(v.Clock.ID.Equals(identity.PublicKey), ShouldBeTrue)
					c.So(v.Clock.Time, ShouldEqual, 1)
				}
				for _, v := range log.FindHeads(log1.Entries) {
					c.So(v.Hash.Equals(log1.Values()[0].Hash), ShouldBeTrue)
				}
			})

			c.Convey("append 100 items to a log", FailureHalts, func(c C) {
				log1 := log.NewLog(ipfs, identity, &log.NewLogOptions{ID: "A"})
				nextPointerAmount := 64

				for i := 0; i < 100; i++ {
					_, err := log1.Append([]byte(fmt.Sprintf("hello%d", i)), nextPointerAmount)
					c.So(err, ShouldBeNil)

					values := log1.Values()
					c.So(len(log.FindHeads(log1.Entries)), ShouldEqual, 1)
					c.So(log.FindHeads(log1.Entries)[0].Hash.Equals(values[len(values)-1].Hash), ShouldBeTrue)
				}

				c.So(len(log1.Entries), ShouldEqual, 100)

				for i, v := range log1.Values() {
					c.So(string(v.Payload), ShouldEqual, fmt.Sprintf("hello%d", i))
					c.So(v.Clock.Time, ShouldEqual, i+1)
					c.So(v.Clock.ID.Equals(identity.PublicKey), ShouldBeTrue)
					c.So(len(v.Next), ShouldEqual, minInt(i, nextPointerAmount))
				}
			})
		})
	})
}

func minInt(a, b int) int {
	if a < b {
		return a
	}
	return b
}
