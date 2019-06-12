package test // import "berty.tech/go-ipfs-log/test"

import (
	"context"
	"fmt"
	"testing"
	"time"

	"berty.tech/go-ipfs-log/entry"
	idp "berty.tech/go-ipfs-log/identityprovider"
	"berty.tech/go-ipfs-log/io"
	ks "berty.tech/go-ipfs-log/keystore"
	cid "github.com/ipfs/go-cid"
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
		ID:       fmt.Sprintf("userA"),
		Type:     "orbitdb",
	})

	if err != nil {
		panic(err)
	}

	Convey("Entry", t, FailureHalts, func(c C) {
		c.Convey("create", FailureHalts, func(c C) {
			c.Convey("creates an empty entry", FailureHalts, func(c C) {
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

			c.Convey("creates an entry with payload", FailureContinues, func(c C) {
				expectedHash := "zdpuAtjiCZSrHRjnxHJkWP6zXYbZnNDv799AZXUkTgdFLfTho"
				e, err := entry.CreateEntry(ipfs, identity, &entry.Entry{Payload: []byte("hello world"), LogID: "A"}, nil)
				c.So(err, ShouldBeNil)

				c.So(string(e.Payload), ShouldEqual, "hello world")
				c.So(e.LogID, ShouldEqual, "A")
				c.So(e.Clock.ID, ShouldResemble, identity.PublicKey)
				c.So(e.Clock.Time, ShouldEqual, 0)
				c.So(e.V, ShouldEqual, 1)
				c.So(len(e.Next), ShouldEqual, 0)
				c.So(e.Hash.String(), ShouldEqual, expectedHash)
			})

			c.Convey("creates an entry with payload and next", FailureContinues, func(c C) {
				expectedHash := "zdpuAsTdJiUff2ymap5cTdLn1yBTWHLoceJ9ikksB2wxrvTPt"
				payload1 := "hello world"
				payload2 := "hello again"
				e1, err := entry.CreateEntry(ipfs, identity, &entry.Entry{Payload: []byte(payload1), LogID: "A"}, nil)
				c.So(err, ShouldBeNil)
				e1.Clock.Tick()
				e2, err := entry.CreateEntry(ipfs, identity, &entry.Entry{Payload: []byte(payload2), LogID: "A", Next: []cid.Cid{e1.Hash}}, e1.Clock)
				c.So(err, ShouldBeNil)

				c.So(string(e2.Payload), ShouldEqual, payload2)
				c.So(len(e2.Next), ShouldEqual, 1)
				c.So(e2.Hash.String(), ShouldEqual, expectedHash)
				c.So(e2.Clock.ID, ShouldResemble, identity.PublicKey)
				c.So(e2.Clock.Time, ShouldEqual, 1)
			})

			c.Convey("should return an entry interopable with older versions", FailureContinues, func(c C) {
				expectedHash := "zdpuArzxF8fqM5E1zE9TgENc6fHqPXBgMKexM4SfoworsKYnt"
				e, err := entry.CreateEntry(ipfs, identity, &entry.Entry{Payload: []byte("hello"), LogID: "A"}, nil)
				c.So(err, ShouldBeNil)

				c.So(e.Hash.String(), ShouldEqual, e.Hash.String())
				c.So(e.Hash.String(), ShouldEqual, expectedHash)
			})

			c.Convey("returns an error if ipfs is not set", FailureContinues, func(c C) {
				e, err := entry.CreateEntry(nil, identity, &entry.Entry{Payload: []byte("hello"), LogID: "A"}, nil)
				c.So(e, ShouldBeNil)
				c.So(err.Error(), ShouldEqual, "ipfs instance not defined")
			})

			c.Convey("returns an error if identity is not set", FailureContinues, func(c C) {
				e, err := entry.CreateEntry(ipfs, nil, &entry.Entry{Payload: []byte("hello"), LogID: "A"}, nil)
				c.So(e, ShouldBeNil)
				c.So(err.Error(), ShouldEqual, "identity is required")
			})

			c.Convey("returns an error if data is not set", FailureContinues, func(c C) {
				e, err := entry.CreateEntry(ipfs, identity, nil, nil)
				c.So(e, ShouldBeNil)
				c.So(err.Error(), ShouldEqual, "data is not defined")
			})

			c.Convey("returns an error if LogID is not set", FailureContinues, func(c C) {
				e, err := entry.CreateEntry(ipfs, identity, &entry.Entry{Payload: []byte("hello")}, nil)
				c.So(e, ShouldBeNil)
				c.So(err.Error(), ShouldEqual, "LogID is required")
			})
		})

		c.Convey("toMultihash", FailureContinues, func(c C) {
			c.Convey("returns an ipfs hash", FailureContinues, func(c C) {
				expectedHash := "zdpuArzxF8fqM5E1zE9TgENc6fHqPXBgMKexM4SfoworsKYnt"
				e, err := entry.CreateEntry(ipfs, identity, &entry.Entry{Payload: []byte("hello"), LogID: "A"}, nil)
				c.So(err, ShouldBeNil)
				hash, err := entry.ToMultihash(ipfs, e)
				c.So(err, ShouldBeNil)

				c.So(e.Hash.String(), ShouldEqual, expectedHash)
				c.So(hash.String(), ShouldEqual, expectedHash)
			})

			// TODO
			c.Convey("returns the correct ipfs hash (multihash) for a v0 entry", FailureContinues, func(c C) {
				expectedHash := "QmV5NpvViHHouBfo7CSnfX2iB4t5PVWNJG8doKt5cwwnxY"
				_ = expectedHash
			})
		})

		// TODO
		c.Convey("fromMultihash", FailureContinues, func(c C) {
		})

		c.Convey("isParent", FailureContinues, func(c C) {
			c.Convey("returns true if entry has a child", FailureContinues, func(c C) {
				payload1 := "hello world"
				payload2 := "hello again"
				e1, err := entry.CreateEntry(ipfs, identity, &entry.Entry{Payload: []byte(payload1), LogID: "A"}, nil)
				c.So(err, ShouldBeNil)
				e2, err := entry.CreateEntry(ipfs, identity, &entry.Entry{Payload: []byte(payload2), LogID: "A", Next: []cid.Cid{e1.Hash}}, nil)
				c.So(err, ShouldBeNil)

				c.So(entry.IsParent(e1, e2), ShouldBeTrue)
			})

			c.Convey("returns false if entry has a child", FailureContinues, func(c C) {
				payload1 := "hello world"
				payload2 := "hello again"
				e1, err := entry.CreateEntry(ipfs, identity, &entry.Entry{Payload: []byte(payload1), LogID: "A"}, nil)
				c.So(err, ShouldBeNil)
				e2, err := entry.CreateEntry(ipfs, identity, &entry.Entry{Payload: []byte(payload2), LogID: "A"}, nil)
				c.So(err, ShouldBeNil)
				e3, err := entry.CreateEntry(ipfs, identity, &entry.Entry{Payload: []byte(payload2), LogID: "A", Next: []cid.Cid{e2.Hash}}, nil)
				c.So(err, ShouldBeNil)

				c.So(entry.IsParent(e1, e2), ShouldBeFalse)
				c.So(entry.IsParent(e1, e3), ShouldBeFalse)
				c.So(entry.IsParent(e2, e3), ShouldBeTrue)
			})
		})

		c.Convey("compare", FailureContinues, func(c C) {
			c.Convey("returns true if entries are the same", FailureContinues, func(c C) {
				payload1 := "hello world"
				e1, err := entry.CreateEntry(ipfs, identity, &entry.Entry{Payload: []byte(payload1), LogID: "A"}, nil)
				c.So(err, ShouldBeNil)
				e2, err := entry.CreateEntry(ipfs, identity, &entry.Entry{Payload: []byte(payload1), LogID: "A"}, nil)
				c.So(err, ShouldBeNil)

				c.So(entry.IsEqual(e1, e2), ShouldBeTrue)
			})

			c.Convey("returns true if entries are not the same", FailureContinues, func(c C) {
				payload1 := "hello world"
				payload2 := "hello again"
				e1, err := entry.CreateEntry(ipfs, identity, &entry.Entry{Payload: []byte(payload1), LogID: "A"}, nil)
				c.So(err, ShouldBeNil)
				e2, err := entry.CreateEntry(ipfs, identity, &entry.Entry{Payload: []byte(payload2), LogID: "A"}, nil)
				c.So(err, ShouldBeNil)

				c.So(entry.IsEqual(e1, e2), ShouldBeFalse)
			})
		})

		// TODO
		c.Convey("isEntry", FailureContinues, func(c C) {
		})
	})
}
