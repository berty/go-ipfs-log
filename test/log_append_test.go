package test // import "berty.tech/go-ipfs-log/test"

import (
	"context"
	"fmt"
	"testing"
	"time"

	idp "berty.tech/go-ipfs-log/identityprovider"
	"berty.tech/go-ipfs-log/keystore"
	"berty.tech/go-ipfs-log/log"
	dssync "github.com/ipfs/go-datastore/sync"

	. "github.com/smartystreets/goconvey/convey"
)

func TestLogAppend(t *testing.T) {
	_, cancel := context.WithTimeout(context.Background(), time.Second*10)
	defer cancel()

	ipfs := NewMemoryServices()

	datastore := dssync.MutexWrap(NewIdentityDataStore())
	keystore, err := keystore.NewKeystore(datastore)
	if err != nil {
		panic(err)
	}

	identity, err := idp.CreateIdentity(&idp.CreateIdentityOptions{
		Keystore: keystore,
		ID:       fmt.Sprintf("userA"),
		Type:     "orbitdb",
	})

	if err != nil {
		panic(err)
	}

	Convey("Log - Append", t, FailureHalts, func(c C) {
		c.Convey("append", FailureHalts, func(c C) {
			c.Convey("append one", FailureHalts, func(c C) {
				log1, err := log.NewLog(ipfs, identity, &log.NewLogOptions{ID: "A"})
				c.So(err, ShouldBeNil)
				_, err = log1.Append([]byte("hello1"), 1)
				c.So(err, ShouldBeNil)

				c.So(log1.Entries.Len(), ShouldEqual, 1)
				values := log1.Values()
				keys := values.Keys()

				for _, k := range keys {
					v := values.UnsafeGet(k)
					c.So(string(v.Payload), ShouldEqual, "hello1")
					c.So(len(v.Next), ShouldEqual, 0)
					c.So(v.Clock.ID, ShouldResemble, identity.PublicKey)
					c.So(v.Clock.Time, ShouldEqual, 1)
				}
				for _, v := range log.FindHeads(log1.Entries) {
					c.So(v.Hash.String(), ShouldEqual, values.UnsafeGet(keys[0]).Hash.String())
				}
			})

			c.Convey("append 100 items to a log", FailureHalts, func(c C) {
				log1, err := log.NewLog(ipfs, identity, &log.NewLogOptions{ID: "A"})
				c.So(err, ShouldBeNil)
				nextPointerAmount := 64

				for i := 0; i < 100; i++ {
					_, err := log1.Append([]byte(fmt.Sprintf("hello%d", i)), nextPointerAmount)
					c.So(err, ShouldBeNil)

					values := log1.Values()
					keys := values.Keys()
					heads := log.FindHeads(log1.Entries)

					c.So(len(heads), ShouldEqual, 1)
					c.So(heads[0].Hash.String(), ShouldEqual, values.UnsafeGet(keys[len(keys)-1]).Hash.String())
				}

				c.So(log1.Entries.Len(), ShouldEqual, 100)

				values := log1.Values()
				keys := values.Keys()

				for i, k := range keys {
					v := values.UnsafeGet(k)

					c.So(string(v.Payload), ShouldEqual, fmt.Sprintf("hello%d", i))
					c.So(v.Clock.Time, ShouldEqual, i+1)
					c.So(v.Clock.ID, ShouldResemble, identity.PublicKey)
					c.So(len(v.Next), ShouldEqual, minInt(i, nextPointerAmount))
				}
			})
		})
	})
}
