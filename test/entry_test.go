package test // import "berty.tech/go-ipfs-log/test"

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/ipfs/go-cid"
	dssync "github.com/ipfs/go-datastore/sync"
	mocknet "github.com/libp2p/go-libp2p/p2p/net/mock"

	ipfslog "berty.tech/go-ipfs-log"
	"berty.tech/go-ipfs-log/entry"
	"berty.tech/go-ipfs-log/errmsg"
	idp "berty.tech/go-ipfs-log/identityprovider"
	"berty.tech/go-ipfs-log/io"
	ks "berty.tech/go-ipfs-log/keystore"

	. "github.com/smartystreets/goconvey/convey"
)

func TestEntry(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*10)
	defer cancel()

	m := mocknet.New(ctx)
	ipfs, closeNode := NewMemoryServices(ctx, t, m)
	defer closeNode()

	datastore := dssync.MutexWrap(NewIdentityDataStore(t))
	keystore, err := ks.NewKeystore(datastore)
	if err != nil {
		t.Fatal(err)
	}

	identity, err := idp.CreateIdentity(&idp.CreateIdentityOptions{
		Keystore: keystore,
		ID:       fmt.Sprintf("userA"),
		Type:     "orbitdb",
	})

	if err != nil {
		t.Fatal(err)
	}

	Convey("Entry", t, FailureHalts, func(c C) {
		c.Convey("create", FailureHalts, func(c C) {
			c.Convey("creates an empty entry", FailureHalts, func(c C) {
				expectedHash := CidB32(t, "zdpuAsPdzSyeux5mFsFV1y3WeHAShGNi4xo22cYBYWUdPtxVB")

				e, err := entry.CreateEntry(ctx, ipfs, identity, &entry.Entry{Payload: []byte("hello"), LogID: "A"}, nil)
				c.So(err, ShouldBeNil)
				c.So(e, ShouldNotBeNil)

				c.So(e.Hash.String(), ShouldEqual, expectedHash)
				c.So(e.LogID, ShouldEqual, "A")
				c.So(e.Clock.GetID(), ShouldResemble, identity.PublicKey)
				c.So(e.Clock.GetTime(), ShouldEqual, 0)
				c.So(e.V, ShouldEqual, 2)
				c.So(string(e.Payload), ShouldEqual, "hello")
				c.So(len(e.Next), ShouldEqual, 0)
				c.So(len(e.Refs), ShouldEqual, 0)
			})

			c.Convey("creates an entry with payload", FailureContinues, func(c C) {
				expectedHash := CidB32(t, "zdpuAyvJU3TS7LUdfRxwAnJorkz6NfpAWHGypsQEXLZxcCCRC")
				e, err := entry.CreateEntry(ctx, ipfs, identity, &entry.Entry{Payload: []byte("hello world"), LogID: "A"}, nil)
				c.So(err, ShouldBeNil)

				c.So(string(e.Payload), ShouldEqual, "hello world")
				c.So(e.LogID, ShouldEqual, "A")
				c.So(e.Clock.GetID(), ShouldResemble, identity.PublicKey)
				c.So(e.Clock.GetTime(), ShouldEqual, 0)
				c.So(e.V, ShouldEqual, 2)
				c.So(len(e.Next), ShouldEqual, 0)
				c.So(len(e.Refs), ShouldEqual, 0)
				c.So(e.Hash.String(), ShouldEqual, expectedHash)
			})

			c.Convey("creates an entry with payload and next", FailureContinues, func(c C) {
				expectedHash := CidB32(t, "zdpuAqsN9Py4EWSfrGYZS8tuokWuiTd9zhS8dhr9XpSGQajP2")
				payload1 := "hello world"
				payload2 := "hello again"
				e1, err := entry.CreateEntry(ctx, ipfs, identity, &entry.Entry{Payload: []byte(payload1), LogID: "A"}, nil)
				c.So(err, ShouldBeNil)
				e1.Clock.Tick()
				e2, err := entry.CreateEntry(ctx, ipfs, identity, &entry.Entry{Payload: []byte(payload2), LogID: "A", Next: []cid.Cid{e1.Hash}, Clock: e1.Clock}, nil)
				c.So(err, ShouldBeNil)

				c.So(string(e2.Payload), ShouldEqual, payload2)
				c.So(len(e2.Next), ShouldEqual, 1)
				c.So(e2.Hash.String(), ShouldEqual, expectedHash)
				c.So(e2.Clock.GetID(), ShouldResemble, identity.PublicKey)
				c.So(e2.Clock.GetTime(), ShouldEqual, 1)
			})

			c.Convey("should return an entry interopable with older versions", FailureContinues, func(c C) {
				expectedHashV1 := CidB32(t, "zdpuAsPdzSyeux5mFsFV1y3WeHAShGNi4xo22cYBYWUdPtxVB")
				entryV1, err := entry.CreateEntry(ctx, ipfs, identity, &entry.Entry{LogID: "A", Payload: []byte("hello")}, nil)
				c.So(err, ShouldBeNil)

				_ = expectedHashV1

				logV1, err := ipfslog.NewFromEntryHash(ctx, ipfs, identity, entryV1.GetHash(), &ipfslog.LogOptions{ID: "A"}, &ipfslog.FetchOptions{})
				c.So(err, ShouldBeNil)

				_ = logV1

				c.So(entryV1.GetHash().String(), ShouldEqual, expectedHashV1)

				id, err := cid.Parse(expectedHashV1)
				c.So(err, ShouldBeNil)

				e, ok := logV1.Get(id)
				c.So(ok, ShouldBeTrue)
				c.So(e.GetHash().String(), ShouldEqual, expectedHashV1)
			})

			c.Convey("returns an error if ipfs is not set", FailureContinues, func(c C) {
				e, err := entry.CreateEntry(ctx, nil, identity, &entry.Entry{Payload: []byte("hello"), LogID: "A"}, nil)
				c.So(e, ShouldBeNil)
				c.So(err, ShouldNotBeNil)
				c.So(err.Error(), ShouldEqual, errmsg.ErrIPFSNotDefined)
			})

			c.Convey("returns an error if identity is not set", FailureContinues, func(c C) {
				e, err := entry.CreateEntry(ctx, ipfs, nil, &entry.Entry{Payload: []byte("hello"), LogID: "A"}, nil)
				c.So(e, ShouldBeNil)
				c.So(err, ShouldNotBeNil)
				c.So(err.Error(), ShouldEqual, errmsg.ErrIdentityNotDefined)
			})

			c.Convey("returns an error if data is not set", FailureContinues, func(c C) {
				e, err := entry.CreateEntry(ctx, ipfs, identity, nil, nil)
				c.So(e, ShouldBeNil)
				c.So(err, ShouldNotBeNil)
				c.So(err.Error(), ShouldEqual, errmsg.ErrPayloadNotDefined)
			})

			c.Convey("returns an error if LogID is not set", FailureContinues, func(c C) {
				e, err := entry.CreateEntry(ctx, ipfs, identity, &entry.Entry{Payload: []byte("hello")}, nil)
				c.So(e, ShouldBeNil)
				c.So(err, ShouldNotBeNil)
				c.So(err.Error(), ShouldEqual, errmsg.ErrLogIDNotDefined)
			})
		})

		c.Convey("toMultihash", FailureContinues, func(c C) {
			c.Convey("returns an ipfs multihash", FailureContinues, func(c C) {
				expectedHash := CidB32(t, "zdpuAsPdzSyeux5mFsFV1y3WeHAShGNi4xo22cYBYWUdPtxVB")
				e, err := entry.CreateEntry(ctx, ipfs, identity, &entry.Entry{Payload: []byte("hello"), LogID: "A"}, nil)
				c.So(err, ShouldBeNil)
				hash, err := e.ToMultihash(ctx, ipfs, nil)
				c.So(err, ShouldBeNil)

				c.So(e.Hash.String(), ShouldEqual, expectedHash)
				c.So(hash.String(), ShouldEqual, expectedHash)
			})

			c.Convey("returns the correct ipfs multihash for a v1 entry", FailureContinues, func(c C) {
				e := getEntriesV1Fixtures(t, identity)[0]
				expectedHash := CidB32(t, "zdpuAsJDrLKrAiU8M518eu6mgv9HzS3e1pfH5XC7LUsFgsK5c")

				hash, err := e.ToMultihash(ctx, ipfs, nil)
				c.So(err, ShouldBeNil)

				c.So(hash.String(), ShouldEqual, expectedHash)
			})

			// TODO
			// 	c.Convey("returns the correct ipfs hash (multihash) for a v0 entry", FailureContinues, func(c C) {
			// 		expectedHash := "QmV5NpvViHHouBfo7CSnfX2iB4t5PVWNJG8doKt5cwwnxY"
			// 		_ = expectedHash
			// 	})
		})

		// TODO
		c.Convey("fromMultihash", FailureContinues, func(c C) {
			c.Convey("creates a entry from ipfs hash", func(c C) {
				expectedHash := CidB32(t, "zdpuAnRGWKPkMHqumqdkRJtzbyW6qAGEiBRv61Zj3Ts4j9tQF")

				payload1 := []byte("hello world")
				payload2 := []byte("hello again")
				entry1, err := entry.CreateEntry(ctx, ipfs, identity, &entry.Entry{Payload: payload1, LogID: "A"}, nil)
				c.So(err, ShouldBeNil)

				entry2, err := entry.CreateEntry(ctx, ipfs, identity, &entry.Entry{Payload: payload2, LogID: "A", Next: []cid.Cid{entry1.Hash}}, nil)
				c.So(err, ShouldBeNil)

				final, err := entry.FromMultihash(ctx, ipfs, entry2.Hash, identity.Provider)
				c.So(err, ShouldBeNil)

				c.So(final.LogID, ShouldEqual, "A")
				c.So(final.Payload, ShouldResemble, payload2)
				c.So(len(final.Next), ShouldEqual, 1)
				c.So(final.Hash.String(), ShouldEqual, expectedHash)
			})

			c.Convey("creates a entry from ipfs multihash of v1 entries", func(c C) {
				expectedHash := CidB32(t, "zdpuAxgKyiM9qkP9yPKCCqrHer9kCqYyr7KbhucsPwwfh6JB3")
				e1 := getEntriesV1Fixtures(t, identity)[0]
				e2 := getEntriesV1Fixtures(t, identity)[1]

				entry1Hash, err := io.WriteCBOR(ctx, ipfs, e1.ToCborEntry(), nil)
				c.So(err, ShouldBeNil)

				entry2Hash, err := io.WriteCBOR(ctx, ipfs, e2.ToCborEntry(), nil)
				c.So(err, ShouldBeNil)

				final, err := entry.FromMultihash(ctx, ipfs, entry2Hash, identity.Provider)
				c.So(err, ShouldBeNil)

				c.So(final.LogID, ShouldEqual, "A")
				c.So(final.Payload, ShouldResemble, e2.Payload)
				c.So(len(final.Next), ShouldEqual, 1)
				c.So(final.Next[0].String(), ShouldEqual, e2.Next[0].String())
				c.So(final.Next[0].String(), ShouldEqual, entry1Hash.String())
				c.So(final.V, ShouldEqual, 1)
				c.So(final.Hash.String(), ShouldEqual, entry2Hash.String())
				c.So(entry2Hash.String(), ShouldEqual, expectedHash)
			})
		})

		c.Convey("isParent", FailureContinues, func(c C) {
			c.Convey("returns true if entry has a child", FailureContinues, func(c C) {
				payload1 := "hello world"
				payload2 := "hello again"
				e1, err := entry.CreateEntry(ctx, ipfs, identity, &entry.Entry{Payload: []byte(payload1), LogID: "A"}, nil)
				c.So(err, ShouldBeNil)
				e2, err := entry.CreateEntry(ctx, ipfs, identity, &entry.Entry{Payload: []byte(payload2), LogID: "A", Next: []cid.Cid{e1.Hash}}, nil)
				c.So(err, ShouldBeNil)

				c.So(e1.IsParent(e2), ShouldBeTrue)
			})

			c.Convey("returns false if entry has a child", FailureContinues, func(c C) {
				payload1 := "hello world"
				payload2 := "hello again"
				e1, err := entry.CreateEntry(ctx, ipfs, identity, &entry.Entry{Payload: []byte(payload1), LogID: "A"}, nil)
				c.So(err, ShouldBeNil)
				e2, err := entry.CreateEntry(ctx, ipfs, identity, &entry.Entry{Payload: []byte(payload2), LogID: "A"}, nil)
				c.So(err, ShouldBeNil)
				e3, err := entry.CreateEntry(ctx, ipfs, identity, &entry.Entry{Payload: []byte(payload2), LogID: "A", Next: []cid.Cid{e2.Hash}}, nil)
				c.So(err, ShouldBeNil)

				c.So(e1.IsParent(e2), ShouldBeFalse)
				c.So(e1.IsParent(e3), ShouldBeFalse)
				c.So(e2.IsParent(e3), ShouldBeTrue)
			})
		})

		c.Convey("compare", FailureContinues, func(c C) {
			c.Convey("returns true if entries are the same", FailureContinues, func(c C) {
				payload1 := "hello world"
				e1, err := entry.CreateEntry(ctx, ipfs, identity, &entry.Entry{Payload: []byte(payload1), LogID: "A"}, nil)
				c.So(err, ShouldBeNil)
				e2, err := entry.CreateEntry(ctx, ipfs, identity, &entry.Entry{Payload: []byte(payload1), LogID: "A"}, nil)
				c.So(err, ShouldBeNil)

				c.So(e1.Equals(e2), ShouldBeTrue)
			})

			c.Convey("returns true if entries are not the same", FailureContinues, func(c C) {
				payload1 := "hello world"
				payload2 := "hello again"
				e1, err := entry.CreateEntry(ctx, ipfs, identity, &entry.Entry{Payload: []byte(payload1), LogID: "A"}, nil)
				c.So(err, ShouldBeNil)
				e2, err := entry.CreateEntry(ctx, ipfs, identity, &entry.Entry{Payload: []byte(payload2), LogID: "A"}, nil)
				c.So(err, ShouldBeNil)

				c.So(e1.Equals(e2), ShouldBeFalse)
			})
		})

		// TODO
		// c.Convey("isEntry", FailureContinues, func(c C) {
		// })
	})
}
