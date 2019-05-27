package test

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/berty/go-ipfs-log/entry"
	idp "github.com/berty/go-ipfs-log/identityprovider"
	"github.com/berty/go-ipfs-log/io"
	ks "github.com/berty/go-ipfs-log/keystore"
	dssync "github.com/ipfs/go-datastore/sync"

	. "github.com/smartystreets/goconvey/convey"
)

func TestEntry(t *testing.T) {
	_, cancel := context.WithTimeout(context.Background(), time.Second*10)
	defer cancel()

	ipfs := io.NewMemoryServices()

	datastore := dssync.MutexWrap(NewIdentityDataStore())
	keystore, err := ks.NewKeystore(datastore)
	if err != nil {
		panic(err)
	}

	identity, err := idp.CreateIdentity(&idp.CreateIdentityOptions{
		Keystore: keystore,
		ID: fmt.Sprintf("userA"),
		Type: "orbitdb",
	})

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
				c.So(e.Clock.ID, ShouldResemble, identity.PublicKey)
				c.So(e.Clock.Time, ShouldEqual, 0)
				c.So(e.V, ShouldEqual, 1)
				c.So(string(e.Payload), ShouldEqual, "hello")
				c.So(len(e.Next), ShouldEqual, 0)
			})
		})
	})
}

