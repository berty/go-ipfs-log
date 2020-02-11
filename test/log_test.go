package test // import "berty.tech/go-ipfs-log/test"

import (
	"context"
	"fmt"
	"strconv"
	"testing"
	"time"

	dssync "github.com/ipfs/go-datastore/sync"
	mocknet "github.com/libp2p/go-libp2p/p2p/net/mock"
	. "github.com/smartystreets/goconvey/convey"

	ipfslog "berty.tech/go-ipfs-log"
	"berty.tech/go-ipfs-log/entry"
	"berty.tech/go-ipfs-log/errmsg"
	idp "berty.tech/go-ipfs-log/identityprovider"
	"berty.tech/go-ipfs-log/iface"
	ks "berty.tech/go-ipfs-log/keystore"
)

func TestLog(t *testing.T) {
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

	var identities []*idp.Identity

	for i := 0; i < 4; i++ {
		char := 'A' + i

		identity, err := idp.CreateIdentity(&idp.CreateIdentityOptions{
			Keystore: keystore,
			ID:       fmt.Sprintf("user%c", char),
			Type:     "orbitdb",
		})

		if err != nil {
			t.Fatal(err)
		}

		identities = append(identities, identity)
	}

	Convey("IPFSLog", t, FailureHalts, func(c C) {
		c.Convey("constructor", FailureHalts, func(c C) {
			c.Convey("sets an id and a clock id", FailureHalts, func(c C) {
				log1, err := ipfslog.NewLog(ipfs, identities[0], &ipfslog.LogOptions{ID: "A"})
				c.So(err, ShouldBeNil)
				c.So(log1.ID, ShouldEqual, "A")
				c.So(log1.Clock.GetID(), ShouldResemble, identities[0].PublicKey)
			})

			c.Convey("sets time.now as id string if id is not passed as an argument", FailureHalts, func(c C) {
				before := time.Now().Unix() / 1000
				log1, err := ipfslog.NewLog(ipfs, identities[0], nil)
				c.So(err, ShouldBeNil)
				after := time.Now().Unix() / 1000

				logid, err := strconv.ParseInt(log1.ID, 10, 64)
				c.So(err, ShouldBeNil)
				c.So(logid, ShouldBeGreaterThanOrEqualTo, before)
				c.So(logid, ShouldBeLessThanOrEqualTo, after)
			})

			c.Convey("sets items if given as params", FailureHalts, func(c C) {
				id1, err := idp.CreateIdentity(&idp.CreateIdentityOptions{
					Keystore: keystore,
					ID:       "userA",
					Type:     "orbitdb",
				})
				c.So(err, ShouldBeNil)
				id2, err := idp.CreateIdentity(&idp.CreateIdentityOptions{
					Keystore: keystore,
					ID:       "userB",
					Type:     "orbitdb",
				})
				c.So(err, ShouldBeNil)
				id3, err := idp.CreateIdentity(&idp.CreateIdentityOptions{
					Keystore: keystore,
					ID:       "userC",
					Type:     "orbitdb",
				})
				c.So(err, ShouldBeNil)
				e1, err := entry.CreateEntry(ctx, ipfs, identities[0], &entry.Entry{Payload: []byte("entryA"), LogID: "A", Clock: entry.NewLamportClock(id1.PublicKey, 0)}, nil)
				c.So(err, ShouldBeNil)
				e2, err := entry.CreateEntry(ctx, ipfs, identities[0], &entry.Entry{Payload: []byte("entryB"), LogID: "A", Clock: entry.NewLamportClock(id2.PublicKey, 1)}, nil)
				c.So(err, ShouldBeNil)
				e3, err := entry.CreateEntry(ctx, ipfs, identities[0], &entry.Entry{Payload: []byte("entryC"), LogID: "A", Clock: entry.NewLamportClock(id3.PublicKey, 2)}, nil)
				c.So(err, ShouldBeNil)

				log1, err := ipfslog.NewLog(ipfs, identities[0], &ipfslog.LogOptions{ID: "A", Entries: entry.NewOrderedMapFromEntries([]iface.IPFSLogEntry{e1, e2, e3})})
				c.So(err, ShouldBeNil)

				values := log1.Values()

				c.So(values.Len(), ShouldEqual, 3)

				keys := values.Keys()
				c.So(string(values.UnsafeGet(keys[0]).GetPayload()), ShouldEqual, "entryA")
				c.So(string(values.UnsafeGet(keys[1]).GetPayload()), ShouldEqual, "entryB")
				c.So(string(values.UnsafeGet(keys[2]).GetPayload()), ShouldEqual, "entryC")
			})

			c.Convey("sets heads if given as params", FailureHalts, func(c C) {
				e1, err := entry.CreateEntry(ctx, ipfs, identities[0], &entry.Entry{Payload: []byte("entryA"), LogID: "A"}, nil)
				c.So(err, ShouldBeNil)
				e2, err := entry.CreateEntry(ctx, ipfs, identities[0], &entry.Entry{Payload: []byte("entryB"), LogID: "A"}, nil)
				c.So(err, ShouldBeNil)
				e3, err := entry.CreateEntry(ctx, ipfs, identities[0], &entry.Entry{Payload: []byte("entryC"), LogID: "A"}, nil)
				c.So(err, ShouldBeNil)

				log1, err := ipfslog.NewLog(ipfs, identities[0], &ipfslog.LogOptions{ID: "B", Entries: entry.NewOrderedMapFromEntries([]iface.IPFSLogEntry{e1, e2, e3}), Heads: []iface.IPFSLogEntry{e3}})
				c.So(err, ShouldBeNil)
				heads := log1.Heads()
				headsKeys := heads.Keys()

				c.So(heads.Len(), ShouldEqual, 1)
				c.So(heads.UnsafeGet(headsKeys[0]).GetHash().String(), ShouldEqual, e3.Hash.String())
			})

			c.Convey("finds heads if heads not given as params", FailureHalts, func(c C) {
				e1, err := entry.CreateEntry(ctx, ipfs, identities[0], &entry.Entry{Payload: []byte("entryA"), LogID: "A"}, nil)
				c.So(err, ShouldBeNil)
				e2, err := entry.CreateEntry(ctx, ipfs, identities[0], &entry.Entry{Payload: []byte("entryB"), LogID: "A"}, nil)
				c.So(err, ShouldBeNil)
				e3, err := entry.CreateEntry(ctx, ipfs, identities[0], &entry.Entry{Payload: []byte("entryC"), LogID: "A"}, nil)
				c.So(err, ShouldBeNil)

				log1, err := ipfslog.NewLog(ipfs, identities[0], &ipfslog.LogOptions{ID: "A", Entries: entry.NewOrderedMapFromEntries([]iface.IPFSLogEntry{e1, e2, e3})})
				c.So(err, ShouldBeNil)
				heads := log1.Heads()

				headsKeys := heads.Keys()

				c.So(heads.Len(), ShouldEqual, 3)
				c.So(heads.UnsafeGet(headsKeys[2]).GetHash().String(), ShouldEqual, e1.Hash.String())
				c.So(heads.UnsafeGet(headsKeys[1]).GetHash().String(), ShouldEqual, e2.Hash.String())
				c.So(heads.UnsafeGet(headsKeys[0]).GetHash().String(), ShouldEqual, e3.Hash.String())
			})

			c.Convey("creates default public AccessController if not defined", FailureHalts, func(c C) {
				log1, err := ipfslog.NewLog(ipfs, identities[0], nil)
				c.So(err, ShouldBeNil)

				err = log1.AccessController.CanAppend(&entry.Entry{Payload: []byte("any")}, identities[0].Provider, nil)
				c.So(err, ShouldBeNil)
			})

			c.Convey("returns an error if ipfs is not net", FailureHalts, func(c C) {
				log1, err := ipfslog.NewLog(nil, identities[0], nil)
				c.So(log1, ShouldBeNil)
				c.So(err.Error(), ShouldEqual, errmsg.ErrIPFSNotDefined)
			})

			c.Convey("returns an error if identity is not net", FailureHalts, func(c C) {
				log1, err := ipfslog.NewLog(ipfs, nil, nil)
				c.So(log1, ShouldBeNil)
				c.So(err.Error(), ShouldEqual, errmsg.ErrIdentityNotDefined)
			})
		})

		c.Convey("toString", FailureHalts, func(c C) {
			expectedData := "five\n└─four\n  └─three\n    └─two\n      └─one"
			log1, err := ipfslog.NewLog(ipfs, identities[0], &ipfslog.LogOptions{ID: "A"})
			c.So(err, ShouldBeNil)
			for _, val := range []string{"one", "two", "three", "four", "five"} {
				_, err := log1.Append(ctx, []byte(val), nil)
				c.So(err, ShouldBeNil)
			}

			c.So(log1.ToString(nil), ShouldEqual, expectedData)
		})
	})
}
