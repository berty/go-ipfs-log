package test // import "berty.tech/go-ipfs-log/test"

import (
	"context"
	"fmt"
	"strconv"
	"testing"
	"time"

	"berty.tech/go-ipfs-log/errmsg"

	"berty.tech/go-ipfs-log/entry"
	idp "berty.tech/go-ipfs-log/identityprovider"
	ks "berty.tech/go-ipfs-log/keystore"
	"berty.tech/go-ipfs-log/log"
	"berty.tech/go-ipfs-log/utils/lamportclock"
	dssync "github.com/ipfs/go-datastore/sync"

	. "github.com/smartystreets/goconvey/convey"
)

func TestLog(t *testing.T) {
	_, cancel := context.WithTimeout(context.Background(), time.Second*10)
	defer cancel()

	ipfs := NewMemoryServices()

	datastore := dssync.MutexWrap(NewIdentityDataStore())
	keystore, err := ks.NewKeystore(datastore)
	if err != nil {
		panic(err)
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
			panic(err)
		}

		identities = append(identities, identity)
	}

	Convey("Log", t, FailureHalts, func(c C) {
		c.Convey("constructor", FailureHalts, func(c C) {
			c.Convey("sets an id and a clock id", FailureHalts, func(c C) {
				log1, err := log.NewLog(ipfs, identities[0], &log.NewLogOptions{ID: "A"})
				c.So(err, ShouldBeNil)
				c.So(log1.ID, ShouldEqual, "A")
				c.So(log1.Clock.ID, ShouldResemble, identities[0].PublicKey)
			})

			c.Convey("sets time.now as id string if id is not passed as an argument", FailureHalts, func(c C) {
				before := time.Now().Unix() / 1000
				log1, err := log.NewLog(ipfs, identities[0], nil)
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
				// TODO: Use time=0 and known public keys for all 3 entries
				e1, err := entry.CreateEntry(ipfs, identities[0], &entry.Entry{Payload: []byte("entryA"), LogID: "A"}, lamportclock.New(id1.PublicKey, 0))
				c.So(err, ShouldBeNil)
				e2, err := entry.CreateEntry(ipfs, identities[0], &entry.Entry{Payload: []byte("entryB"), LogID: "A"}, lamportclock.New(id2.PublicKey, 1))
				c.So(err, ShouldBeNil)
				e3, err := entry.CreateEntry(ipfs, identities[0], &entry.Entry{Payload: []byte("entryC"), LogID: "A"}, lamportclock.New(id3.PublicKey, 2))
				c.So(err, ShouldBeNil)

				log1, err := log.NewLog(ipfs, identities[0], &log.NewLogOptions{ID: "A", Entries: entry.NewOrderedMapFromEntries([]*entry.Entry{e1, e2, e3})})
				c.So(err, ShouldBeNil)

				values := log1.Values()

				c.So(values.Len(), ShouldEqual, 3)

				keys := values.Keys()
				c.So(string(values.UnsafeGet(keys[0]).Payload), ShouldEqual, "entryA")
				c.So(string(values.UnsafeGet(keys[1]).Payload), ShouldEqual, "entryB")
				c.So(string(values.UnsafeGet(keys[2]).Payload), ShouldEqual, "entryC")
			})

			c.Convey("sets heads if given as params", FailureHalts, func(c C) {
				e1, err := entry.CreateEntry(ipfs, identities[0], &entry.Entry{Payload: []byte("entryA"), LogID: "A"}, nil)
				c.So(err, ShouldBeNil)
				e2, err := entry.CreateEntry(ipfs, identities[0], &entry.Entry{Payload: []byte("entryB"), LogID: "A"}, nil)
				c.So(err, ShouldBeNil)
				e3, err := entry.CreateEntry(ipfs, identities[0], &entry.Entry{Payload: []byte("entryC"), LogID: "A"}, nil)
				c.So(err, ShouldBeNil)

				log1, err := log.NewLog(ipfs, identities[0], &log.NewLogOptions{ID: "B", Entries: entry.NewOrderedMapFromEntries([]*entry.Entry{e1, e2, e3}), Heads: []*entry.Entry{e3}})
				c.So(err, ShouldBeNil)
				heads := log1.Heads()
				headsKeys := heads.Keys()

				c.So(heads.Len(), ShouldEqual, 1)
				c.So(heads.UnsafeGet(headsKeys[0]).Hash.String(), ShouldEqual, e3.Hash.String())
			})

			c.Convey("finds heads if heads not given as params", FailureHalts, func(c C) {
				e1, err := entry.CreateEntry(ipfs, identities[0], &entry.Entry{Payload: []byte("entryA"), LogID: "A"}, nil)
				c.So(err, ShouldBeNil)
				e2, err := entry.CreateEntry(ipfs, identities[0], &entry.Entry{Payload: []byte("entryB"), LogID: "A"}, nil)
				c.So(err, ShouldBeNil)
				e3, err := entry.CreateEntry(ipfs, identities[0], &entry.Entry{Payload: []byte("entryC"), LogID: "A"}, nil)
				c.So(err, ShouldBeNil)

				log1, err := log.NewLog(ipfs, identities[0], &log.NewLogOptions{ID: "A", Entries: entry.NewOrderedMapFromEntries([]*entry.Entry{e1, e2, e3})})
				c.So(err, ShouldBeNil)
				heads := log1.Heads()

				headsKeys := heads.Keys()

				c.So(heads.Len(), ShouldEqual, 3)
				c.So(heads.UnsafeGet(headsKeys[2]).Hash.String(), ShouldEqual, e1.Hash.String())
				c.So(heads.UnsafeGet(headsKeys[1]).Hash.String(), ShouldEqual, e2.Hash.String())
				c.So(heads.UnsafeGet(headsKeys[0]).Hash.String(), ShouldEqual, e3.Hash.String())
			})

			c.Convey("creates default public AccessController if not defined", FailureHalts, func(c C) {
				log1, err := log.NewLog(ipfs, identities[0], nil)
				c.So(err, ShouldBeNil)

				err = log1.AccessController.CanAppend(&entry.Entry{Payload: []byte("any")}, identities[0])
				c.So(err, ShouldBeNil)
			})

			c.Convey("returns an error if ipfs is not net", FailureHalts, func(c C) {
				log1, err := log.NewLog(nil, identities[0], nil)
				c.So(log1, ShouldBeNil)
				c.So(err.Error(), ShouldEqual, errmsg.IPFSNotDefined.Error())
			})

			c.Convey("returns an error if identity is not net", FailureHalts, func(c C) {
				log1, err := log.NewLog(ipfs, nil, nil)
				c.So(log1, ShouldBeNil)
				c.So(err.Error(), ShouldEqual, errmsg.IdentityNotDefined.Error())
			})
		})

		c.Convey("toString", FailureHalts, func(c C) {
			expectedData := "five\n└─four\n  └─three\n    └─two\n      └─one"
			log1, err := log.NewLog(ipfs, identities[0], &log.NewLogOptions{ID: "A"})
			c.So(err, ShouldBeNil)
			for _, val := range []string{"one", "two", "three", "four", "five"} {
				_, err := log1.Append([]byte(val), 1)
				c.So(err, ShouldBeNil)
			}

			c.So(log1.ToString(nil), ShouldEqual, expectedData)
		})
	})
}
