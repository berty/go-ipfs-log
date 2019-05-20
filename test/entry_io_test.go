package test

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/berty/go-ipfs-log/entry"
	idp "github.com/berty/go-ipfs-log/identityprovider"
	io "github.com/berty/go-ipfs-log/io"
	ks "github.com/berty/go-ipfs-log/keystore"
	"github.com/berty/go-ipfs-log/log"
	cid "github.com/ipfs/go-cid"
	ds "github.com/ipfs/go-datastore"
	dssync "github.com/ipfs/go-datastore/sync"

	. "github.com/smartystreets/goconvey/convey"
)

func TestPersistency(t *testing.T) {
	_, cancel := context.WithTimeout(context.Background(), time.Second*10)
	defer cancel()

	ipfs := io.NewMemoryServices()

	datastore := dssync.MutexWrap(ds.NewMapDatastore())
	keystore, err := ks.NewKeystore(datastore)
	if err != nil {
		panic(err)
	}

	idProvider := idp.NewOrbitDBIdentityProvider(keystore)

	var identities []*idp.Identity

	for i := 0; i < 4; i++ {
		identity, err := idProvider.GetID(fmt.Sprintf("User%d", i))
		if err != nil {
			panic(err)
		}

		identities = append(identities, identity)
	}

	Convey("Entry - Persistency", t, FailureHalts, func(c C) {
		c.Convey("log with 1 entry", FailureHalts, func(c C) {
			log1 := log.NewLog(ipfs, identities[0], &log.NewLogOptions{ID: "X"})
			e, err := log1.Append([]byte("one"), 1)
			c.So(err, ShouldBeNil)

			hash := e.Hash
			res := entry.FetchAll(ipfs, []cid.Cid{hash}, &entry.FetchOptions{})
			c.So(len(res), ShouldEqual, 1)
		})

		c.Convey("log with 2 entries", FailureHalts, func(c C) {
			log1 := log.NewLog(ipfs, identities[0], &log.NewLogOptions{ID: "X"})
			_, err := log1.Append([]byte("one"), 1)
			c.So(err, ShouldBeNil)
			e, err := log1.Append([]byte("two"), 1)
			c.So(err, ShouldBeNil)

			hash := e.Hash
			res := entry.FetchAll(ipfs, []cid.Cid{hash}, &entry.FetchOptions{})
			c.So(len(res), ShouldEqual, 2)
		})

		c.Convey("loads max 1 entry from a log of 2 entries", FailureHalts, func(c C) {
			log1 := log.NewLog(ipfs, identities[0], &log.NewLogOptions{ID: "X"})
			_, err := log1.Append([]byte("one"), 1)
			c.So(err, ShouldBeNil)
			e, err := log1.Append([]byte("two"), 1)
			c.So(err, ShouldBeNil)

			hash := e.Hash
			res := entry.FetchAll(ipfs, []cid.Cid{hash}, &entry.FetchOptions{Length: 1})
			c.So(len(res), ShouldEqual, 1)
		})

		c.Convey("log with 100 entries", FailureHalts, func(c C) {
			var e *entry.Entry
			var err error

			log1 := log.NewLog(ipfs, identities[0], &log.NewLogOptions{ID: "X"})
			for i := 0; i < 100; i++ {
				e, err = log1.Append([]byte(fmt.Sprintf("hello%d", i)), 1)
				c.So(err, ShouldBeNil)
			}

			hash := e.Hash
			res := entry.FetchAll(ipfs, []cid.Cid{hash}, &entry.FetchOptions{})
			c.So(len(res), ShouldEqual, 100)
		})

		c.Convey("load only 42 entries from a log with 100 entries", FailureHalts, func(c C) {
			log1 := log.NewLog(ipfs, identities[0], &log.NewLogOptions{ID: "X"})
			log2 := log.NewLog(ipfs, identities[0], &log.NewLogOptions{ID: "X"})
			for i := 0; i < 100; i++ {
				_, err := log1.Append([]byte(fmt.Sprintf("hello%d", i)), 1)
				c.So(err, ShouldBeNil)
				if i%10 == 0 {
					heads := append(log.FindHeads(log2.Entries), log.FindHeads(log1.Entries)...)
					log2 := log.NewLog(ipfs, identities[0], &log.NewLogOptions{ID: log2.ID, Entries: log2.Values(), Heads: heads})
					_, err := log2.Append([]byte(fmt.Sprintf("hi%d", i)), 1)
					c.So(err, ShouldBeNil)
				}
			}

			hash, err := log2.ToMultihash()
			c.So(err, ShouldBeNil)

			res, err := log.NewFromMultihash(ipfs, identities[0], hash, &log.NewLogOptions{}, &log.FetchOptions{Length: 42})
			c.So(err, ShouldBeNil)
			c.So(len(res.Entries), ShouldEqual, 42)
		})
	})
}
