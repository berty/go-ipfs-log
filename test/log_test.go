package test

import (
	"context"
	"fmt"
	"strconv"
	"testing"
	"time"

	"github.com/berty/go-ipfs-log/entry"
	idp "github.com/berty/go-ipfs-log/identityprovider"
	io "github.com/berty/go-ipfs-log/io"
	ks "github.com/berty/go-ipfs-log/keystore"
	"github.com/berty/go-ipfs-log/log"
	"github.com/berty/go-ipfs-log/utils/lamportclock"
	ds "github.com/ipfs/go-datastore"
	dssync "github.com/ipfs/go-datastore/sync"

	. "github.com/smartystreets/goconvey/convey"
)

func TestLog(t *testing.T) {
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

	Convey("Log", t, FailureHalts, func(c C) {
		c.Convey("constructor", FailureHalts, func(c C) {
			c.Convey("sets an id and a clock id", FailureHalts, func(c C) {
				log1, err := log.NewLog(ipfs, identities[0], &log.NewLogOptions{ID: "A"})
				c.So(err, ShouldBeNil)
				c.So(log1.ID, ShouldEqual, "A")
				c.So(log1.Clock.ID.Equals(identities[0].PublicKey), ShouldBeTrue)
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
				id1, err := idProvider.GetID("A")
				c.So(err, ShouldBeNil)
				id2, err := idProvider.GetID("B")
				c.So(err, ShouldBeNil)
				id3, err := idProvider.GetID("C")
				c.So(err, ShouldBeNil)
				e1, err := entry.CreateEntry(ipfs, identities[0], &entry.Entry{Payload: []byte("entryA"), LogID: "A"}, lamportclock.New(id1.PublicKey, 0))
				c.So(err, ShouldBeNil)
				e2, err := entry.CreateEntry(ipfs, identities[0], &entry.Entry{Payload: []byte("entryB"), LogID: "A"}, lamportclock.New(id2.PublicKey, 0))
				c.So(err, ShouldBeNil)
				e3, err := entry.CreateEntry(ipfs, identities[0], &entry.Entry{Payload: []byte("entryC"), LogID: "A"}, lamportclock.New(id3.PublicKey, 0))
				c.So(err, ShouldBeNil)

				log1, err := log.NewLog(ipfs, identities[0], &log.NewLogOptions{ID: "A", Entries: []*entry.Entry{e1, e2, e3}})
				c.So(err, ShouldBeNil)

				c.So(len(log1.Values()), ShouldEqual, 3)
				c.So(string(log1.Values()[0].Payload), ShouldEqual, "entryA")
				c.So(string(log1.Values()[1].Payload), ShouldEqual, "entryB")
				c.So(string(log1.Values()[2].Payload), ShouldEqual, "entryC")
			})

			c.Convey("sets heads if given as params", FailureHalts, func(c C) {
				e1, err := entry.CreateEntry(ipfs, identities[0], &entry.Entry{Payload: []byte("entryA"), LogID: "A"}, nil)
				c.So(err, ShouldBeNil)
				e2, err := entry.CreateEntry(ipfs, identities[0], &entry.Entry{Payload: []byte("entryB"), LogID: "A"}, nil)
				c.So(err, ShouldBeNil)
				e3, err := entry.CreateEntry(ipfs, identities[0], &entry.Entry{Payload: []byte("entryC"), LogID: "A"}, nil)
				c.So(err, ShouldBeNil)

				log1, err := log.NewLog(ipfs, identities[0], &log.NewLogOptions{ID: "B", Entries: []*entry.Entry{e1, e2, e3}, Heads: []*entry.Entry{e3}})
				c.So(err, ShouldBeNil)
				heads := log.FindHeads(log1.Entries)

				c.So(len(heads), ShouldEqual, 1)
				c.So(heads[0].Hash.String(), ShouldEqual, e3.Hash.String())
			})

			c.Convey("finds heads if heads not given as params", FailureHalts, func(c C) {
				e1, err := entry.CreateEntry(ipfs, identities[0], &entry.Entry{Payload: []byte("entryA"), LogID: "A"}, nil)
				c.So(err, ShouldBeNil)
				e2, err := entry.CreateEntry(ipfs, identities[0], &entry.Entry{Payload: []byte("entryB"), LogID: "A"}, nil)
				c.So(err, ShouldBeNil)
				e3, err := entry.CreateEntry(ipfs, identities[0], &entry.Entry{Payload: []byte("entryC"), LogID: "A"}, nil)
				c.So(err, ShouldBeNil)

				log1, err := log.NewLog(ipfs, identities[0], &log.NewLogOptions{ID: "A", Entries: []*entry.Entry{e1, e2, e3}})
				c.So(err, ShouldBeNil)
				heads := log.FindHeads(log1.Entries)

				c.So(len(heads), ShouldEqual, 3)
				c.So(heads[2].Hash.String(), ShouldEqual, e1.Hash.String())
				c.So(heads[1].Hash.String(), ShouldEqual, e2.Hash.String())
				c.So(heads[0].Hash.String(), ShouldEqual, e3.Hash.String())
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
				c.So(err.Error(), ShouldEqual, "ipfs instance not defined")
			})

			c.Convey("returns an error if identity is not net", FailureHalts, func(c C) {
				log1, err := log.NewLog(ipfs, nil, nil)
				c.So(log1, ShouldBeNil)
				c.So(err.Error(), ShouldEqual, "identity is required")
			})
		})
	})
}
