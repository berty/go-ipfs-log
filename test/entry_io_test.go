package test

import (
	"context"
	"fmt"
	"reflect"
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

func TestEntryPersistency(t *testing.T) {
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

	Convey("Entry - Persistency", t, FailureContinues, func(c C) {
		c.Convey("log with 1 entry", FailureContinues, func(c C) {
			log1, err := log.NewLog(ipfs, identities[0], &log.NewLogOptions{ID: "X"})
			c.So(err, ShouldBeNil)
			e, err := log1.Append([]byte("one"), 1)
			c.So(err, ShouldBeNil)

			hash := e.Hash
			res := entry.FetchAll(ipfs, []cid.Cid{hash}, &entry.FetchOptions{})
			c.So(len(res), ShouldEqual, 1)
		})

		c.Convey("log with 2 entries", FailureContinues, func(c C) {
			log1, err := log.NewLog(ipfs, identities[0], &log.NewLogOptions{ID: "X"})
			c.So(err, ShouldBeNil)
			_, err = log1.Append([]byte("one"), 1)
			c.So(err, ShouldBeNil)
			e, err := log1.Append([]byte("two"), 1)
			c.So(err, ShouldBeNil)

			hash := e.Hash
			res := entry.FetchAll(ipfs, []cid.Cid{hash}, &entry.FetchOptions{})
			c.So(len(res), ShouldEqual, 2)
		})

		c.Convey("loads max 1 entry from a log of 2 entries", FailureContinues, func(c C) {
			log1, err := log.NewLog(ipfs, identities[0], &log.NewLogOptions{ID: "X"})
			c.So(err, ShouldBeNil)
			_, err = log1.Append([]byte("one"), 1)
			c.So(err, ShouldBeNil)
			e, err := log1.Append([]byte("two"), 1)
			c.So(err, ShouldBeNil)

			hash := e.Hash
			res := entry.FetchAll(ipfs, []cid.Cid{hash}, &entry.FetchOptions{Length: 1})
			c.So(len(res), ShouldEqual, 1)
		})

		c.Convey("log with 100 entries", FailureContinues, func(c C) {
			var e *entry.Entry
			var err error

			log1, err := log.NewLog(ipfs, identities[0], &log.NewLogOptions{ID: "X"})
			c.So(err, ShouldBeNil)
			for i := 0; i < 100; i++ {
				e, err = log1.Append([]byte(fmt.Sprintf("hello%d", i)), 1)
				c.So(err, ShouldBeNil)
			}

			hash := e.Hash
			res := entry.FetchAll(ipfs, []cid.Cid{hash}, &entry.FetchOptions{})
			c.So(len(res), ShouldEqual, 100)
		})

		c.Convey("load only 42 entries from a log with 100 entries", FailureContinues, func(c C) {
			log1, err := log.NewLog(ipfs, identities[0], &log.NewLogOptions{ID: "X"})
			c.So(err, ShouldBeNil)
			log2, err := log.NewLog(ipfs, identities[0], &log.NewLogOptions{ID: "X"})
			c.So(err, ShouldBeNil)

			for i := 0; i < 100; i++ {
				_, err := log1.Append([]byte(fmt.Sprintf("hello%d", i)), 1)
				c.So(err, ShouldBeNil)
				if i%10 == 0 {
					heads := append(log.FindHeads(log2.Entries), log.FindHeads(log1.Entries)...)
					log2, err = log.NewLog(ipfs, identities[0], &log.NewLogOptions{ID: log2.ID, Entries: log2.Values(), Heads: heads})
					c.So(err, ShouldBeNil)
					_, err := log2.Append([]byte(fmt.Sprintf("hi%d", i)), 1)
					c.So(err, ShouldBeNil)
				}
			}

			hash, err := log1.ToMultihash()
			c.So(err, ShouldBeNil)

			res, err := log.NewFromMultihash(ipfs, identities[0], hash, &log.NewLogOptions{}, &log.FetchOptions{Length: 42})
			c.So(err, ShouldBeNil)
			c.So(res.Entries.Len(), ShouldEqual, 42)
		})

		c.Convey("load only 99 entries from a log with 100 entries", FailureContinues, func(c C) {
			log1, err := log.NewLog(ipfs, identities[0], &log.NewLogOptions{ID: "X"})
			c.So(err, ShouldBeNil)
			log2, err := log.NewLog(ipfs, identities[0], &log.NewLogOptions{ID: "X"})
			c.So(err, ShouldBeNil)

			for i := 0; i < 100; i++ {
				_, err := log1.Append([]byte(fmt.Sprintf("hello%d", i)), 1)
				c.So(err, ShouldBeNil)
				if i%10 == 0 {
					log2, err = log.NewLog(ipfs, identities[0], &log.NewLogOptions{ID: log2.ID, Entries: log2.Values()})
					c.So(err, ShouldBeNil)
					_, err := log2.Append([]byte(fmt.Sprintf("hi%d", i)), 1)
					c.So(err, ShouldBeNil)
					_, err = log2.Join(log1, -1)
					c.So(err, ShouldBeNil)
				}
			}

			hash, err := log2.ToMultihash()
			c.So(err, ShouldBeNil)

			res, err := log.NewFromMultihash(ipfs, identities[0], hash, &log.NewLogOptions{}, &log.FetchOptions{Length: 99})
			c.So(err, ShouldBeNil)
			c.So(res.Entries.Len(), ShouldEqual, 99)
		})

		c.Convey("load only 10 entries from a log with 100 entries", FailureContinues, func(c C) {
			log1, err := log.NewLog(ipfs, identities[0], &log.NewLogOptions{ID: "X"})
			c.So(err, ShouldBeNil)
			log2, err := log.NewLog(ipfs, identities[0], &log.NewLogOptions{ID: "X"})
			c.So(err, ShouldBeNil)
			log3, err := log.NewLog(ipfs, identities[0], &log.NewLogOptions{ID: "X"})
			c.So(err, ShouldBeNil)

			for i := 0; i < 100; i++ {
				_, err := log1.Append([]byte(fmt.Sprintf("hello%d", i)), 1)
				c.So(err, ShouldBeNil)
				if i%10 == 0 {
					log2, err = log.NewLog(ipfs, identities[0], &log.NewLogOptions{ID: log2.ID, Entries: log2.Values(), Heads: log.FindHeads(log2.Entries)})
					c.So(err, ShouldBeNil)
					_, err := log2.Append([]byte(fmt.Sprintf("hi%d", i)), 1)
					c.So(err, ShouldBeNil)
					_, err = log2.Join(log1, -1)
					c.So(err, ShouldBeNil)
				}
				if i%25 == 0 {
					heads := append(log.FindHeads(log3.Entries), log.FindHeads(log2.Entries)...)
					log3, err = log.NewLog(ipfs, identities[0], &log.NewLogOptions{ID: log3.ID, Entries: log3.Values(), Heads: heads})
					c.So(err, ShouldBeNil)
					_, err := log3.Append([]byte(fmt.Sprintf("--%d", i)), 1)
					c.So(err, ShouldBeNil)
				}
			}

			_, err = log3.Join(log2, -1)
			c.So(err, ShouldBeNil)

			hash, err := log3.ToMultihash()
			c.So(err, ShouldBeNil)

			res, err := log.NewFromMultihash(ipfs, identities[0], hash, &log.NewLogOptions{}, &log.FetchOptions{Length: 10})
			c.So(err, ShouldBeNil)
			c.So(res.Entries.Len(), ShouldEqual, 10)
		})

		c.Convey("load only 10 entries and then expand to max from a log with 100 entries", FailureContinues, func(c C) {
			log1, err := log.NewLog(ipfs, identities[0], &log.NewLogOptions{ID: "X"})
			c.So(err, ShouldBeNil)
			log2, err := log.NewLog(ipfs, identities[1], &log.NewLogOptions{ID: "X"})
			c.So(err, ShouldBeNil)
			log3, err := log.NewLog(ipfs, identities[2], &log.NewLogOptions{ID: "X"})
			c.So(err, ShouldBeNil)

			for i := 0; i < 30; i++ {
				_, err := log1.Append([]byte(fmt.Sprintf("hello%d", i)), 1)
				c.So(err, ShouldBeNil)
				if i%10 == 0 {
					_, err := log2.Append([]byte(fmt.Sprintf("hi%d", i)), 1)
					c.So(err, ShouldBeNil)
					_, err = log2.Join(log1, -1)
					c.So(err, ShouldBeNil)
				}
				if i%25 == 0 {
					heads := append(log.FindHeads(log3.Entries), log.FindHeads(log2.Entries)...)
					log3, err = log.NewLog(ipfs, identities[2], &log.NewLogOptions{ID: log3.ID, Entries: log3.Values(), Heads: heads})
					c.So(err, ShouldBeNil)
					_, err := log3.Append([]byte(fmt.Sprintf("--%d", i)), 1)
					c.So(err, ShouldBeNil)
				}
			}

			_, err = log3.Join(log2, -1)
			c.So(err, ShouldBeNil)

			log4, err := log.NewLog(ipfs, identities[3], &log.NewLogOptions{ID: "X"})
			c.So(err, ShouldBeNil)
			_, err = log4.Join(log2, -1)
			c.So(err, ShouldBeNil)
			_, err = log4.Join(log3, -1)
			c.So(err, ShouldBeNil)

			var values3, values4 [][]byte
			log3Values := log3.Values()
			log3Keys := log3Values.Keys()

			log4Values := log4.Values()
			log4Keys := log4Values.Keys()

			for _, k := range log3Keys {
				v, _ := log3Values.Get(k)
				values3 = append(values3, v.Payload)
			}
			for _, k := range log4Keys {
				v, _ := log4Values.Get(k)
				values4 = append(values4, v.Payload)
			}
			c.So(reflect.DeepEqual(values3, values4), ShouldBeTrue)
		})
	})
}
