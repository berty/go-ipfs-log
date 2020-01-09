package test

import (
	"context"
	"fmt"
	"testing"
	"time"

	ipfslog "berty.tech/go-ipfs-log"
	"berty.tech/go-ipfs-log/entry"
	idp "berty.tech/go-ipfs-log/identity"
	"berty.tech/go-ipfs-log/keystore"
	dssync "github.com/ipfs/go-datastore/sync"
	. "github.com/smartystreets/goconvey/convey"
)

func TestLogAppend(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*10)
	defer cancel()

	ipfs := NewMemoryServices()

	datastore := dssync.MutexWrap(NewIdentityDataStore())
	keystore, err := keystore.New(datastore)
	if err != nil {
		panic(err)
	}

	identity, err := idp.CreateIdentity(keystore, fmt.Sprintf("userA"))
	if err != nil {
		panic(err)
	}

	Convey("IPFSLog - Append", t, FailureHalts, func(c C) {
		c.Convey("append", FailureHalts, func(c C) {
			c.Convey("append one", FailureHalts, func(c C) {
				log1, err := ipfslog.NewLog(ipfs, identity, &ipfslog.LogOptions{ID: "A"})
				c.So(err, ShouldBeNil)
				_, err = log1.Append(ctx, []byte("hello1"), 1)
				c.So(err, ShouldBeNil)

				c.So(log1.Entries.Len(), ShouldEqual, 1)
				values := log1.Values()
				keys := values.Keys()

				for _, k := range keys {
					v := values.UnsafeGet(k)
					c.So(string(v.GetPayload()), ShouldEqual, "hello1")
					c.So(len(v.GetNext()), ShouldEqual, 0)
					c.So(v.GetClock().GetID(), ShouldResemble, identity.PublicKey)
					c.So(v.GetClock().GetTime(), ShouldEqual, 1)
				}
				for _, v := range entry.FindHeads(log1.Entries) {
					c.So(v.GetHash().String(), ShouldEqual, values.UnsafeGet(keys[0]).GetHash().String())
				}
			})

			c.Convey("append 100 items to a log", FailureHalts, func(c C) {
				log1, err := ipfslog.NewLog(ipfs, identity, &ipfslog.LogOptions{ID: "A"})
				c.So(err, ShouldBeNil)
				nextPointerAmount := 64

				for i := 0; i < 100; i++ {
					_, err := log1.Append(ctx, []byte(fmt.Sprintf("hello%d", i)), nextPointerAmount)
					c.So(err, ShouldBeNil)

					values := log1.Values()
					keys := values.Keys()
					heads := entry.FindHeads(log1.Entries)

					c.So(len(heads), ShouldEqual, 1)
					c.So(heads[0].GetHash().String(), ShouldEqual, values.UnsafeGet(keys[len(keys)-1]).GetHash().String())
				}

				c.So(log1.Entries.Len(), ShouldEqual, 100)

				values := log1.Values()
				keys := values.Keys()

				for i, k := range keys {
					v := values.UnsafeGet(k)

					c.So(string(v.GetPayload()), ShouldEqual, fmt.Sprintf("hello%d", i))
					c.So(v.GetClock().GetTime(), ShouldEqual, i+1)
					c.So(v.GetClock().GetID(), ShouldResemble, identity.PublicKey)
					c.So(len(v.GetNext()), ShouldEqual, minInt(i, nextPointerAmount))
				}
			})
		})
	})
}
