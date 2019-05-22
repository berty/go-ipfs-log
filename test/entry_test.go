package test

import (
	"context"
	"testing"
	"time"

	"github.com/berty/go-ipfs-log/entry"
	idp "github.com/berty/go-ipfs-log/identityprovider"
	io "github.com/berty/go-ipfs-log/io"
	ks "github.com/berty/go-ipfs-log/keystore"
	ds "github.com/ipfs/go-datastore"
	dssync "github.com/ipfs/go-datastore/sync"

	. "github.com/smartystreets/goconvey/convey"
)

func TestEntry(t *testing.T) {
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

	Convey("Entry", t, FailureContinues, func(c C) {
		c.Convey("create", FailureContinues, func(c C) {
			c.Convey("creates a an empty entry", FailureContinues, func(c C) {
				expectedHash := "zdpuArzxF8fqM5E1zE9TgENc6fHqPXBgMKexM4SfoworsKYnt"
				e, err := entry.CreateEntry(ipfs, identity, &entry.Entry{Payload: []byte("hello"), LogID: "A"}, nil)
				c.So(err, ShouldBeNil)

				c.So(e.Hash.String(), ShouldEqual, expectedHash)
				c.So(e.LogID, ShouldEqual, "A")
				c.So(e.Clock.ID.Equals(identity.PublicKey), ShouldBeTrue)
				c.So(e.Clock.Time, ShouldEqual, 0)
				c.So(e.V, ShouldEqual, 1)
				c.So(string(e.Payload), ShouldEqual, "hello")
				c.So(len(e.Next), ShouldEqual, 0)
			})
		})
	})
}

