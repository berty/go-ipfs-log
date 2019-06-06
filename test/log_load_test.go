package test

import (
	"context"
	"fmt"
	"github.com/berty/go-ipfs-log/entry"
	idp "github.com/berty/go-ipfs-log/identityprovider"
	"github.com/berty/go-ipfs-log/io"
	ks "github.com/berty/go-ipfs-log/keystore"
	"github.com/berty/go-ipfs-log/log"
	"github.com/berty/go-ipfs-log/test/logcreator"
	dssync "github.com/ipfs/go-datastore/sync"
	"testing"
	"time"

	. "github.com/smartystreets/goconvey/convey"
)

//const BadComparatorReturnsZero = (a, b) => 0

func TestLogLoad(t *testing.T) {
	_, cancel := context.WithTimeout(context.Background(), time.Second*10)
	defer cancel()

	ipfs := io.NewMemoryServices()

	datastore := dssync.MutexWrap(NewIdentityDataStore())
	keystore, err := ks.NewKeystore(datastore)
	if err != nil {
		panic(err)
	}

	var identities [4]*idp.Identity

	for i, char := range []rune{'C', 'B', 'D', 'A'} {
		identity, err := idp.CreateIdentity(&idp.CreateIdentityOptions{
			Keystore: keystore,
			ID:       fmt.Sprintf("user%c", char),
			Type:     "orbitdb",
		})

		if err != nil {
			panic(err)
		}

		identities[i] = identity
	}

	firstWriteExpectedData := []string{
		"entryA6", "entryA7", "entryA8", "entryA9",
		"entryA10", "entryB1", "entryB2", "entryB3",
		"entryB4", "entryB5", "entryA1", "entryA2",
		"entryA3", "entryA4", "entryA5", "entryC0",
	}

	Convey("Log - Load", t, FailureHalts, func(c C) {
		c.Convey("fromJSON", FailureHalts, func(c C) {
			c.Convey("creates a log from an entry", FailureHalts, func(c C) {
				fixture, err := logcreator.CreateLogWithSixteenEntries(ipfs, identities)
				c.So(err, ShouldBeNil)

				data := fixture.Log
				json := fixture.JSON

				// TODO: Is this useless?
				//heads := map[string]*entry.Entry{}
				//
				//for _, h := range json.Heads {
				//	e, err := entry.FromMultihash(ipfs, h, identities[0].Provider)
				//	c.So(err, ShouldBeNil)
				//
				//	heads[e.Hash.String()] = e
				//}

				l, err := log.NewFromJSON(ipfs, identities[0], json, &log.NewLogOptions{ID: "X"}, &entry.FetchOptions{})
				c.So(err, ShouldBeNil)

				values := l.Values()

				c.So(l.ID, ShouldEqual, data.Heads().At(0).LogID)
				c.So(values.Len(), ShouldEqual, 16)
				var foundEntries []string
				for _, k := range values.Keys() {
					foundEntries = append(foundEntries, string(values.UnsafeGet(k).Payload))
				}

				c.So(foundEntries, ShouldResemble, fixture.ExpectedData)
			})

			c.Convey("creates a log from an entry with custom tiebreaker", FailureHalts, func(c C) {
				fixture, err := logcreator.CreateLogWithSixteenEntries(ipfs, identities)
				c.So(err, ShouldBeNil)

				data := fixture.Log
				json := fixture.JSON

				println("")
				l, err := log.NewFromJSON(ipfs, identities[0], json, &log.NewLogOptions{ID: "X", SortFn: log.FirstWriteWins}, &entry.FetchOptions{Length: intPtr(-1)})
				c.So(err, ShouldBeNil)

				values := l.Values()

				c.So(l.ID, ShouldEqual, data.Heads().At(0).LogID)
				c.So(values.Len(), ShouldEqual, 16)
				var foundEntries []string
				for _, k := range values.Keys() {
					foundEntries = append(foundEntries, string(values.UnsafeGet(k).Payload))
				}

				// TODO: found out why firstWriteExpectedData is what it is in JS test
				c.So(foundEntries, ShouldResemble, firstWriteExpectedData)
				_ = firstWriteExpectedData
			})
		})

		c.Convey("fromEntryHash", FailureHalts, func(c C) {
			c.Convey("creates a log from an entry hash", FailureHalts, func(c C) {
				fixture, err := logcreator.CreateLogWithSixteenEntries(ipfs, identities)
				c.So(err, ShouldBeNil)

				data := fixture.Log
				json := fixture.JSON

				log1, err := log.NewFromEntryHash(ipfs, identities[0], json.Heads[0], &log.NewLogOptions{ ID: "X" }, &log.FetchOptions{})
				log2, err := log.NewFromEntryHash(ipfs, identities[0], json.Heads[1], &log.NewLogOptions{ ID: "X" }, &log.FetchOptions{})

				_, err = log1.Join(log2, -1)
				c.So(err, ShouldBeNil)

				values := log1.Values()

				c.So(log1.ID, ShouldEqual, data.Heads().At(0).LogID)
				c.So(values.Len(), ShouldEqual, 16)

				var foundEntries []string
				for _, k := range values.Keys() {
					foundEntries = append(foundEntries, string(values.UnsafeGet(k).Payload))
				}

				c.So(foundEntries, ShouldResemble, fixture.ExpectedData)
			})
		})
	})
}