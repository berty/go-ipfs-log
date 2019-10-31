package test // import "berty.tech/go-ipfs-log/test"

import (
	"berty.tech/go-ipfs-log/iface"
	"bytes"
	"context"
	"fmt"
	"math/rand"
	_ "sort"
	"strings"
	"testing"
	"time"

	ipfslog "berty.tech/go-ipfs-log"

	"berty.tech/go-ipfs-log/entry/sorting"

	"berty.tech/go-ipfs-log/entry"
	idp "berty.tech/go-ipfs-log/identityprovider"
	ks "berty.tech/go-ipfs-log/keystore"
	"berty.tech/go-ipfs-log/test/logcreator"
	"github.com/ipfs/go-cid"
	dssync "github.com/ipfs/go-datastore/sync"

	. "github.com/smartystreets/goconvey/convey"
)

func BadComparatorReturnsZero(a, b iface.IPFSLogEntry) (int, error) {
	return 0, nil
}

func TestLogLoad(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*10)
	defer cancel()

	ipfs := NewMemoryServices()

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

	_ = firstWriteExpectedData

	Convey("IPFSLog - Load", t, FailureHalts, func(c C) {
		c.Convey("fromJSON", FailureHalts, func(c C) {
			c.Convey("creates a log from an entry", FailureHalts, func(c C) {
				fixture, err := logcreator.CreateLogWithSixteenEntries(ctx, ipfs, identities)
				c.So(err, ShouldBeNil)

				data := fixture.Log
				json := fixture.JSON

				// TODO: Is this useless?
				//heads := map[string]*entry.Entry{}
				//
				//for _, h := range json.Heads {
				//	e, err := entry.fromMultihash(ipfs, h, identities[0].Provider)
				//	c.So(err, ShouldBeNil)
				//
				//	heads[e.Hash.String()] = e
				//}

				l, err := ipfslog.NewFromJSON(ctx, ipfs, identities[0], json, &ipfslog.LogOptions{ID: "X"}, &entry.FetchOptions{})
				c.So(err, ShouldBeNil)

				values := l.Values()

				c.So(l.ID, ShouldEqual, data.Heads().At(0).GetLogID())
				c.So(values.Len(), ShouldEqual, 16)
				c.So(entriesAsStrings(values), ShouldResemble, fixture.ExpectedData)
			})

			c.Convey("creates a log from an entry with custom tiebreaker", FailureHalts, func(c C) {
				fixture, err := logcreator.CreateLogWithSixteenEntries(ctx, ipfs, identities)
				c.So(err, ShouldBeNil)

				data := fixture.Log
				json := fixture.JSON

				l, err := ipfslog.NewFromJSON(ctx, ipfs, identities[0], json, &ipfslog.LogOptions{ID: "X", SortFn: sorting.FirstWriteWins}, &entry.FetchOptions{Length: intPtr(-1)})
				c.So(err, ShouldBeNil)

				c.So(l.ID, ShouldEqual, data.Heads().At(0).GetLogID())
				c.So(l.Values().Len(), ShouldEqual, 16)
				// TODO: found out why firstWriteExpectedData is what it is in JS test

				c.So(entriesAsStrings(l.Values()), ShouldResemble, firstWriteExpectedData)
				_ = firstWriteExpectedData
			})
		})

		c.Convey("fromEntryHash", FailureHalts, func(c C) {
			c.Convey("creates a log from an entry hash", FailureHalts, func(c C) {
				fixture, err := logcreator.CreateLogWithSixteenEntries(ctx, ipfs, identities)
				c.So(err, ShouldBeNil)

				data := fixture.Log
				json := fixture.JSON

				log1, err := ipfslog.NewFromEntryHash(ctx, ipfs, identities[0], json.Heads[0], &ipfslog.LogOptions{ID: "X"}, &ipfslog.FetchOptions{})
				log2, err := ipfslog.NewFromEntryHash(ctx, ipfs, identities[0], json.Heads[1], &ipfslog.LogOptions{ID: "X"}, &ipfslog.FetchOptions{})

				_, err = log1.Join(log2, -1)
				c.So(err, ShouldBeNil)

				c.So(log1.ID, ShouldEqual, data.Heads().At(0).GetLogID())
				c.So(log1.Values().Len(), ShouldEqual, 16)
				c.So(entriesAsStrings(log1.Values()), ShouldResemble, fixture.ExpectedData)
			})

			c.Convey("creates a log from an entry hash with custom tiebreaker", FailureHalts, func(c C) {
				fixture, err := logcreator.CreateLogWithSixteenEntries(ctx, ipfs, identities)
				c.So(err, ShouldBeNil)

				data := fixture.Log
				json := fixture.JSON

				log1, err := ipfslog.NewFromEntryHash(ctx, ipfs, identities[0], json.Heads[0], &ipfslog.LogOptions{ID: "X", SortFn: sorting.FirstWriteWins}, &ipfslog.FetchOptions{})
				log2, err := ipfslog.NewFromEntryHash(ctx, ipfs, identities[0], json.Heads[1], &ipfslog.LogOptions{ID: "X", SortFn: sorting.FirstWriteWins}, &ipfslog.FetchOptions{})

				_, err = log1.Join(log2, -1)
				c.So(err, ShouldBeNil)

				c.So(log1.ID, ShouldEqual, data.Heads().At(0).GetLogID())
				c.So(log1.Values().Len(), ShouldEqual, 16)
				c.So(entriesAsStrings(log1.Values()), ShouldResemble, firstWriteExpectedData)
			})
		})

		c.Convey("fromEntry", FailureHalts, func(c C) {
			resortedIdentities := [4]*idp.Identity{identities[2], identities[1], identities[0], identities[3]}

			c.Convey("creates a log from an entry", FailureHalts, func(c C) {
				fixture, err := logcreator.CreateLogWithSixteenEntries(ctx, ipfs, resortedIdentities)
				c.So(err, ShouldBeNil)

				data := fixture.Log

				l, err := ipfslog.NewFromEntry(ctx, ipfs, identities[0], data.Heads().Slice(), &ipfslog.LogOptions{}, &entry.FetchOptions{})
				c.So(err, ShouldBeNil)

				c.So(l.ID, ShouldEqual, data.Heads().At(0).GetLogID())
				c.So(l.Values().Len(), ShouldEqual, 16)
				c.So(entriesAsStrings(l.Values()), ShouldResemble, fixture.ExpectedData)
			})

			c.Convey("creates a log from an entry with custom tiebreaker", FailureHalts, func(c C) {
				fixture, err := logcreator.CreateLogWithSixteenEntries(ctx, ipfs, resortedIdentities)
				c.So(err, ShouldBeNil)

				data := fixture.Log

				l, err := ipfslog.NewFromEntry(ctx, ipfs, identities[0], data.Heads().Slice(), &ipfslog.LogOptions{SortFn: sorting.FirstWriteWins}, &entry.FetchOptions{Length: intPtr(-1)})
				c.So(err, ShouldBeNil)

				c.So(l.ID, ShouldEqual, data.Heads().At(0).GetLogID())
				c.So(l.Values().Len(), ShouldEqual, 16)
				c.So(entriesAsStrings(l.Values()), ShouldResemble, firstWriteExpectedData)
			})

			c.Convey("keeps the original heads", FailureHalts, func(c C) {
				fixture, err := logcreator.CreateLogWithSixteenEntries(ctx, ipfs, resortedIdentities)
				c.So(err, ShouldBeNil)

				data := fixture.Log

				log1, err := ipfslog.NewFromEntry(ctx, ipfs, identities[0], data.Heads().Slice(), &ipfslog.LogOptions{}, &entry.FetchOptions{Length: intPtr(data.Heads().Len())})

				c.So(err, ShouldBeNil)
				c.So(log1.ID, ShouldEqual, data.Heads().At(0).GetLogID())
				c.So(log1.Values().Len(), ShouldEqual, data.Heads().Len())
				c.So(string(log1.Values().At(0).GetPayload()), ShouldEqual, "entryC0")
				c.So(string(log1.Values().At(1).GetPayload()), ShouldEqual, "entryA10")

				log2, err := ipfslog.NewFromEntry(ctx, ipfs, identities[0], data.Heads().Slice(), &ipfslog.LogOptions{}, &entry.FetchOptions{Length: intPtr(4)})

				c.So(err, ShouldBeNil)
				c.So(log2.ID, ShouldEqual, data.Heads().At(0).GetLogID())
				c.So(log2.Values().Len(), ShouldEqual, 4)
				c.So(string(log2.Values().At(0).GetPayload()), ShouldEqual, "entryC0")
				c.So(string(log2.Values().At(1).GetPayload()), ShouldEqual, "entryA8")
				c.So(string(log2.Values().At(2).GetPayload()), ShouldEqual, "entryA9")
				c.So(string(log2.Values().At(3).GetPayload()), ShouldEqual, "entryA10")

				log3, err := ipfslog.NewFromEntry(ctx, ipfs, identities[0], data.Heads().Slice(), &ipfslog.LogOptions{}, &entry.FetchOptions{Length: intPtr(7)})

				c.So(err, ShouldBeNil)
				c.So(log3.ID, ShouldEqual, data.Heads().At(0).GetLogID())
				c.So(log3.Values().Len(), ShouldEqual, 7)
				c.So(string(log3.Values().At(0).GetPayload()), ShouldEqual, "entryB5")
				c.So(string(log3.Values().At(1).GetPayload()), ShouldEqual, "entryA6")
				c.So(string(log3.Values().At(2).GetPayload()), ShouldEqual, "entryC0")
				c.So(string(log3.Values().At(3).GetPayload()), ShouldEqual, "entryA7")
				c.So(string(log3.Values().At(4).GetPayload()), ShouldEqual, "entryA8")
				c.So(string(log3.Values().At(5).GetPayload()), ShouldEqual, "entryA9")
				c.So(string(log3.Values().At(6).GetPayload()), ShouldEqual, "entryA10")
			})

			c.Convey("onProgress callback is fired for each entry", FailureHalts, func(c C) {
				// TODO: skipped
			})

			c.Convey("retrieves partial log from an entry hash", FailureHalts, func(c C) {
				log1, err := ipfslog.NewLog(ipfs, identities[0], &ipfslog.LogOptions{ID: "X"})
				c.So(err, ShouldBeNil)

				log2, err := ipfslog.NewLog(ipfs, identities[0], &ipfslog.LogOptions{ID: "X"})
				c.So(err, ShouldBeNil)

				log3, err := ipfslog.NewLog(ipfs, identities[0], &ipfslog.LogOptions{ID: "X"})
				c.So(err, ShouldBeNil)

				var items1 []iface.IPFSLogEntry
				var items2 []iface.IPFSLogEntry
				var items3 []iface.IPFSLogEntry

				const amount = 100
				for i := 1; i <= amount; i++ {
					prev1 := lastEntry(items1)
					prev2 := lastEntry(items2)
					prev3 := lastEntry(items3)

					var nexts []cid.Cid
					if prev1 != nil {
						nexts = []cid.Cid{prev1.GetHash()}
					}

					n1, err := entry.CreateEntry(ctx, ipfs, log1.Identity, &entry.Entry{LogID: "X", Payload: []byte(fmt.Sprintf("entryA%d", i)), Next: nexts}, nil)
					c.So(err, ShouldBeNil)

					if prev2 != nil {
						nexts = []cid.Cid{prev2.GetHash(), n1.Hash}
					} else {
						nexts = []cid.Cid{n1.Hash}
					}

					n2, err := entry.CreateEntry(ctx, ipfs, log2.Identity, &entry.Entry{LogID: "X", Payload: []byte(fmt.Sprintf("entryB%d", i)), Next: nexts}, nil)
					c.So(err, ShouldBeNil)

					if prev3 != nil {
						nexts = []cid.Cid{prev3.GetHash(), n1.Hash, n2.Hash}
					} else {
						nexts = []cid.Cid{n1.Hash, n2.Hash}
					}

					n3, err := entry.CreateEntry(ctx, ipfs, log3.Identity, &entry.Entry{LogID: "X", Payload: []byte(fmt.Sprintf("entryC%d", i)), Next: nexts}, nil)
					c.So(err, ShouldBeNil)

					items1 = append(items1, n1)
					items2 = append(items2, n2)
					items3 = append(items3, n3)
				}

				// limit to 10 entries
				a, err := ipfslog.NewFromEntry(ctx, ipfs, identities[0], []iface.IPFSLogEntry{lastEntry(items1)}, &ipfslog.LogOptions{}, &entry.FetchOptions{Length: intPtr(10)})
				c.So(err, ShouldBeNil)
				c.So(a.Values().Len(), ShouldEqual, 10)

				// limit to 42 entries
				b, err := ipfslog.NewFromEntry(ctx, ipfs, identities[0], []iface.IPFSLogEntry{lastEntry(items1)}, &ipfslog.LogOptions{}, &entry.FetchOptions{Length: intPtr(42)})
				c.So(err, ShouldBeNil)
				c.So(b.Values().Len(), ShouldEqual, 42)
			})

			c.Convey("retrieves full log from an entry hash", FailureHalts, func(c C) {
				log1, err := ipfslog.NewLog(ipfs, identities[0], &ipfslog.LogOptions{ID: "X"})
				c.So(err, ShouldBeNil)

				log2, err := ipfslog.NewLog(ipfs, identities[0], &ipfslog.LogOptions{ID: "X"})
				c.So(err, ShouldBeNil)

				log3, err := ipfslog.NewLog(ipfs, identities[0], &ipfslog.LogOptions{ID: "X"})
				c.So(err, ShouldBeNil)

				var items1 []iface.IPFSLogEntry
				var items2 []iface.IPFSLogEntry
				var items3 []iface.IPFSLogEntry

				const amount = 100
				for i := 1; i <= amount; i++ {
					prev1 := lastEntry(items1)
					prev2 := lastEntry(items2)
					prev3 := lastEntry(items3)

					var nexts []cid.Cid
					if prev1 != nil {
						nexts = []cid.Cid{prev1.GetHash()}
					}

					n1, err := entry.CreateEntry(ctx, ipfs, log1.Identity, &entry.Entry{LogID: "X", Payload: []byte(fmt.Sprintf("entryA%d", i)), Next: nexts}, nil)
					c.So(err, ShouldBeNil)

					if prev2 != nil {
						nexts = []cid.Cid{prev2.GetHash(), n1.Hash}
					} else {
						nexts = []cid.Cid{n1.Hash}
					}

					n2, err := entry.CreateEntry(ctx, ipfs, log2.Identity, &entry.Entry{LogID: "X", Payload: []byte(fmt.Sprintf("entryB%d", i)), Next: nexts}, nil)
					c.So(err, ShouldBeNil)

					if prev3 != nil {
						nexts = []cid.Cid{prev3.GetHash(), n2.Hash}
					} else {
						nexts = []cid.Cid{n2.Hash}
					}

					n3, err := entry.CreateEntry(ctx, ipfs, log3.Identity, &entry.Entry{LogID: "X", Payload: []byte(fmt.Sprintf("entryC%d", i)), Next: nexts}, nil)
					c.So(err, ShouldBeNil)

					items1 = append(items1, n1)
					items2 = append(items2, n2)
					items3 = append(items3, n3)
				}

				lA, err := ipfslog.NewFromEntry(ctx, ipfs, identities[0], []iface.IPFSLogEntry{lastEntry(items1)}, &ipfslog.LogOptions{}, &entry.FetchOptions{Length: intPtr(amount * 1)})
				c.So(err, ShouldBeNil)
				c.So(lA.Values().Len(), ShouldEqual, amount)

				lB, err := ipfslog.NewFromEntry(ctx, ipfs, identities[0], []iface.IPFSLogEntry{lastEntry(items2)}, &ipfslog.LogOptions{}, &entry.FetchOptions{Length: intPtr(amount * 2)})
				c.So(err, ShouldBeNil)
				c.So(lB.Values().Len(), ShouldEqual, amount*2)

				lC, err := ipfslog.NewFromEntry(ctx, ipfs, identities[0], []iface.IPFSLogEntry{lastEntry(items3)}, &ipfslog.LogOptions{}, &entry.FetchOptions{Length: intPtr(amount * 3)})
				c.So(err, ShouldBeNil)
				c.So(lC.Values().Len(), ShouldEqual, amount*3)
			})

			c.Convey("retrieves full log from an entry hash 2", FailureHalts, func(c C) {
				log1, err := ipfslog.NewLog(ipfs, identities[0], &ipfslog.LogOptions{ID: "X"})
				c.So(err, ShouldBeNil)

				log2, err := ipfslog.NewLog(ipfs, identities[0], &ipfslog.LogOptions{ID: "X"})
				c.So(err, ShouldBeNil)

				log3, err := ipfslog.NewLog(ipfs, identities[0], &ipfslog.LogOptions{ID: "X"})
				c.So(err, ShouldBeNil)

				var items1 []iface.IPFSLogEntry
				var items2 []iface.IPFSLogEntry
				var items3 []iface.IPFSLogEntry

				const amount = 100
				for i := 1; i <= amount; i++ {
					prev1 := lastEntry(items1)
					prev2 := lastEntry(items2)
					prev3 := lastEntry(items3)

					var nexts []cid.Cid
					if prev1 != nil {
						nexts = []cid.Cid{prev1.GetHash()}
					}

					n1, err := entry.CreateEntry(ctx, ipfs, log1.Identity, &entry.Entry{LogID: "X", Payload: []byte(fmt.Sprintf("entryA%d", i)), Next: nexts}, nil)
					c.So(err, ShouldBeNil)

					if prev2 != nil {
						nexts = []cid.Cid{prev2.GetHash(), n1.Hash}
					} else {
						nexts = []cid.Cid{n1.Hash}
					}

					n2, err := entry.CreateEntry(ctx, ipfs, log2.Identity, &entry.Entry{LogID: "X", Payload: []byte(fmt.Sprintf("entryB%d", i)), Next: nexts}, nil)
					c.So(err, ShouldBeNil)

					if prev3 != nil {
						nexts = []cid.Cid{prev3.GetHash(), n1.Hash, n2.Hash}
					} else {
						nexts = []cid.Cid{n1.Hash, n2.Hash}
					}

					n3, err := entry.CreateEntry(ctx, ipfs, log3.Identity, &entry.Entry{LogID: "X", Payload: []byte(fmt.Sprintf("entryC%d", i)), Next: nexts}, nil)
					c.So(err, ShouldBeNil)

					items1 = append(items1, n1)
					items2 = append(items2, n2)
					items3 = append(items3, n3)
				}

				lA, err := ipfslog.NewFromEntry(ctx, ipfs, identities[0], []iface.IPFSLogEntry{lastEntry(items1)}, &ipfslog.LogOptions{}, &entry.FetchOptions{Length: intPtr(amount * 1)})
				c.So(err, ShouldBeNil)
				c.So(lA.Values().Len(), ShouldEqual, amount)

				lB, err := ipfslog.NewFromEntry(ctx, ipfs, identities[1], []iface.IPFSLogEntry{lastEntry(items2)}, &ipfslog.LogOptions{}, &entry.FetchOptions{Length: intPtr(amount * 2)})
				c.So(err, ShouldBeNil)
				c.So(lB.Values().Len(), ShouldEqual, amount*2)

				lC, err := ipfslog.NewFromEntry(ctx, ipfs, identities[2], []iface.IPFSLogEntry{lastEntry(items3)}, &ipfslog.LogOptions{}, &entry.FetchOptions{Length: intPtr(amount * 3)})
				c.So(err, ShouldBeNil)
				c.So(lC.Values().Len(), ShouldEqual, amount*3)
			})

			c.Convey("retrieves full log from an entry hash 3", FailureHalts, func(c C) {
				log1, err := ipfslog.NewLog(ipfs, identities[0], &ipfslog.LogOptions{ID: "X"})
				c.So(err, ShouldBeNil)

				log2, err := ipfslog.NewLog(ipfs, identities[1], &ipfslog.LogOptions{ID: "X"})
				c.So(err, ShouldBeNil)

				log3, err := ipfslog.NewLog(ipfs, identities[3], &ipfslog.LogOptions{ID: "X"})
				c.So(err, ShouldBeNil)

				var items1 []iface.IPFSLogEntry
				var items2 []iface.IPFSLogEntry
				var items3 []iface.IPFSLogEntry

				const amount = 10
				for i := 1; i <= amount; i++ {
					prev1 := lastEntry(items1)
					prev2 := lastEntry(items2)
					prev3 := lastEntry(items3)

					var nexts []cid.Cid

					log1.Clock.Tick()
					log2.Clock.Tick()
					log3.Clock.Tick()

					if prev1 != nil {
						nexts = []cid.Cid{prev1.GetHash()}
					}

					n1, err := entry.CreateEntry(ctx, ipfs, log1.Identity, &entry.Entry{LogID: "X", Payload: []byte(fmt.Sprintf("entryA%d", i)), Next: nexts}, log1.Clock)
					c.So(err, ShouldBeNil)

					if prev2 != nil {
						nexts = []cid.Cid{prev2.GetHash(), n1.Hash}
					} else {
						nexts = []cid.Cid{n1.Hash}
					}

					n2, err := entry.CreateEntry(ctx, ipfs, log2.Identity, &entry.Entry{LogID: "X", Payload: []byte(fmt.Sprintf("entryB%d", i)), Next: nexts}, log2.Clock)
					c.So(err, ShouldBeNil)

					if prev3 != nil {
						nexts = []cid.Cid{prev3.GetHash(), n1.Hash, n2.Hash}
					} else {
						nexts = []cid.Cid{n1.Hash, n2.Hash}
					}

					n3, err := entry.CreateEntry(ctx, ipfs, log3.Identity, &entry.Entry{LogID: "X", Payload: []byte(fmt.Sprintf("entryC%d", i)), Next: nexts}, log3.Clock)
					c.So(err, ShouldBeNil)

					log1.Clock.Merge(log2.Clock)
					log1.Clock.Merge(log3.Clock)
					log2.Clock.Merge(log1.Clock)
					log2.Clock.Merge(log3.Clock)
					log3.Clock.Merge(log1.Clock)
					log3.Clock.Merge(log2.Clock)

					items1 = append(items1, n1)
					items2 = append(items2, n2)
					items3 = append(items3, n3)
				}

				lA, err := ipfslog.NewFromEntry(ctx, ipfs, identities[0], []iface.IPFSLogEntry{lastEntry(items1)}, &ipfslog.LogOptions{}, &entry.FetchOptions{Length: intPtr(amount * 1)})
				c.So(err, ShouldBeNil)
				c.So(lA.Values().Len(), ShouldEqual, amount)

				itemsInB := []string{
					"entryA1",
					"entryB1",
					"entryA2",
					"entryB2",
					"entryA3",
					"entryB3",
					"entryA4",
					"entryB4",
					"entryA5",
					"entryB5",
					"entryA6",
					"entryB6",
					"entryA7",
					"entryB7",
					"entryA8",
					"entryB8",
					"entryA9",
					"entryB9",
					"entryA10",
					"entryB10",
				}

				lB, err := ipfslog.NewFromEntry(ctx, ipfs, identities[1], []iface.IPFSLogEntry{lastEntry(items2)}, &ipfslog.LogOptions{}, &entry.FetchOptions{Length: intPtr(amount * 2)})
				c.So(err, ShouldBeNil)
				c.So(lB.Values().Len(), ShouldEqual, amount*2)
				c.So(entriesAsStrings(lB.Values()), ShouldResemble, itemsInB)

				lC, err := ipfslog.NewFromEntry(ctx, ipfs, identities[3], []iface.IPFSLogEntry{lastEntry(items3)}, &ipfslog.LogOptions{}, &entry.FetchOptions{Length: intPtr(amount * 3)})
				c.So(err, ShouldBeNil)

				_, err = lC.Append(ctx, []byte("EOF"), 1)
				c.So(err, ShouldBeNil)

				c.So(lC.Values().Len(), ShouldEqual, amount*3+1)

				tmp := []string{
					"entryA1",
					"entryB1",
					"entryC1",
					"entryA2",
					"entryB2",
					"entryC2",
					"entryA3",
					"entryB3",
					"entryC3",
					"entryA4",
					"entryB4",
					"entryC4",
					"entryA5",
					"entryB5",
					"entryC5",
					"entryA6",
					"entryB6",
					"entryC6",
					"entryA7",
					"entryB7",
					"entryC7",
					"entryA8",
					"entryB8",
					"entryC8",
					"entryA9",
					"entryB9",
					"entryC9",
					"entryA10",
					"entryB10",
					"entryC10",
					"EOF",
				}

				c.So(entriesAsStrings(lC.Values()), ShouldResemble, tmp)

				// make sure logX comes after A, B and C
				logX, err := ipfslog.NewLog(ipfs, identities[3], &ipfslog.LogOptions{ID: "X"})
				c.So(err, ShouldBeNil)

				_, err = logX.Append(ctx, []byte{'1'}, 1)
				c.So(err, ShouldBeNil)

				_, err = logX.Append(ctx, []byte{'2'}, 1)
				c.So(err, ShouldBeNil)

				_, err = logX.Append(ctx, []byte{'3'}, 1)
				c.So(err, ShouldBeNil)

				lD, err := ipfslog.NewFromEntry(ctx, ipfs, identities[2], []iface.IPFSLogEntry{lastEntry(logX.Values().Slice())}, &ipfslog.LogOptions{}, &entry.FetchOptions{Length: intPtr(-1)})
				c.So(err, ShouldBeNil)

				_, err = lC.Join(lD, -1)
				c.So(err, ShouldBeNil)

				_, err = lD.Join(lC, -1)
				c.So(err, ShouldBeNil)

				_, err = lC.Append(ctx, []byte("DONE"), 1)
				c.So(err, ShouldBeNil)

				_, err = lD.Append(ctx, []byte("DONE"), 1)
				c.So(err, ShouldBeNil)

				logF, err := ipfslog.NewFromEntry(ctx, ipfs, identities[2], []iface.IPFSLogEntry{lastEntry(lC.Values().Slice())}, &ipfslog.LogOptions{}, &entry.FetchOptions{Length: intPtr(-1), Exclude: nil})
				c.So(err, ShouldBeNil)

				logG, err := ipfslog.NewFromEntry(ctx, ipfs, identities[2], []iface.IPFSLogEntry{lastEntry(lD.Values().Slice())}, &ipfslog.LogOptions{}, &entry.FetchOptions{Length: intPtr(-1), Exclude: nil})
				c.So(err, ShouldBeNil)

				c.So(logF.ToString(nil), ShouldEqual, bigLogString)
				c.So(logG.ToString(nil), ShouldEqual, bigLogString)
			})

			c.Convey("retrieves full log of randomly joined log", FailureHalts, func(c C) {
				log1, err := ipfslog.NewLog(ipfs, identities[0], &ipfslog.LogOptions{ID: "X"})
				c.So(err, ShouldBeNil)

				log2, err := ipfslog.NewLog(ipfs, identities[1], &ipfslog.LogOptions{ID: "X"})
				c.So(err, ShouldBeNil)

				log3, err := ipfslog.NewLog(ipfs, identities[3], &ipfslog.LogOptions{ID: "X"})
				c.So(err, ShouldBeNil)

				for i := 1; i <= 5; i++ {
					_, err := log1.Append(ctx, []byte(fmt.Sprintf("entryA%d", i)), 1)
					c.So(err, ShouldBeNil)

					_, err = log2.Append(ctx, []byte(fmt.Sprintf("entryB%d", i)), 1)
					c.So(err, ShouldBeNil)
				}

				_, err = log3.Join(log1, -1)
				c.So(err, ShouldBeNil)

				_, err = log3.Join(log2, -1)
				c.So(err, ShouldBeNil)

				for i := 6; i <= 10; i++ {
					_, err := log1.Append(ctx, []byte(fmt.Sprintf("entryA%d", i)), 1)
					c.So(err, ShouldBeNil)
				}

				_, err = log1.Join(log3, -1)

				for i := 11; i <= 15; i++ {
					_, err := log1.Append(ctx, []byte(fmt.Sprintf("entryA%d", i)), 1)
					c.So(err, ShouldBeNil)
				}

				expectedData := []string{"entryA1", "entryB1", "entryA2", "entryB2",
					"entryA3", "entryB3", "entryA4", "entryB4",
					"entryA5", "entryB5",
					"entryA6", "entryA7", "entryA8", "entryA9", "entryA10",
					"entryA11", "entryA12", "entryA13", "entryA14", "entryA15",
				}

				c.So(entriesAsStrings(log1.Values()), ShouldResemble, expectedData)
			})

			c.Convey("retrieves randomly joined log deterministically", FailureHalts, func(c C) {
				logA, err := ipfslog.NewLog(ipfs, identities[0], &ipfslog.LogOptions{ID: "X"})
				c.So(err, ShouldBeNil)

				logB, err := ipfslog.NewLog(ipfs, identities[2], &ipfslog.LogOptions{ID: "X"})
				c.So(err, ShouldBeNil)

				log3, err := ipfslog.NewLog(ipfs, identities[3], &ipfslog.LogOptions{ID: "X"})
				c.So(err, ShouldBeNil)

				l, err := ipfslog.NewLog(ipfs, identities[1], &ipfslog.LogOptions{ID: "X"})
				c.So(err, ShouldBeNil)

				for i := 1; i <= 5; i++ {
					_, err := logA.Append(ctx, []byte(fmt.Sprintf("entryA%d", i)), 1)
					c.So(err, ShouldBeNil)

					_, err = logB.Append(ctx, []byte(fmt.Sprintf("entryB%d", i)), 1)
					c.So(err, ShouldBeNil)
				}

				_, err = log3.Join(logA, -1)
				c.So(err, ShouldBeNil)

				_, err = log3.Join(logB, -1)
				c.So(err, ShouldBeNil)

				for i := 6; i <= 10; i++ {
					_, err := logA.Append(ctx, []byte(fmt.Sprintf("entryA%d", i)), 1)
					c.So(err, ShouldBeNil)
				}

				_, err = l.Join(log3, -1)
				c.So(err, ShouldBeNil)

				_, err = l.Append(ctx, []byte("entryC0"), 1)
				c.So(err, ShouldBeNil)

				_, err = l.Join(logA, 16)
				c.So(err, ShouldBeNil)

				expectedData := []string{
					"entryA1", "entryB1", "entryA2", "entryB2",
					"entryA3", "entryB3", "entryA4", "entryB4",
					"entryA5", "entryB5",
					"entryA6",
					"entryC0", "entryA7", "entryA8", "entryA9", "entryA10",
				}

				c.So(entriesAsStrings(l.Values()), ShouldResemble, expectedData)
			})

			c.Convey("sorts", FailureHalts, func(c C) {
				testLog, err := logcreator.CreateLogWithSixteenEntries(ctx, ipfs, resortedIdentities)
				c.So(err, ShouldBeNil)

				l := testLog.Log
				expectedData := testLog.ExpectedData

				expectedData2 := []string{
					"entryA1", "entryB1", "entryA2", "entryB2",
					"entryA3", "entryB3", "entryA4", "entryB4",
					"entryA5", "entryB5",
					"entryA6", "entryA7", "entryA8", "entryA9", "entryA10",
				}

				expectedData3 := []string{
					"entryA1", "entryB1", "entryA2", "entryB2",
					"entryA3", "entryB3", "entryA4", "entryB4",
					"entryA5", "entryB5", "entryA6", "entryC0",
					"entryA7", "entryA8", "entryA9",
				}

				expectedData4 := []string{
					"entryA1", "entryB1", "entryA2", "entryB2",
					"entryA3", "entryB3", "entryA4", "entryB4",
					"entryA5", "entryA6", "entryC0", "entryA7",
					"entryA8", "entryA9", "entryA10",
				}

				fetchOrder := l.Values().Slice()
				sorting.Sort(sorting.Compare, fetchOrder)
				c.So(entriesAsStrings(entry.NewOrderedMapFromEntries(fetchOrder)), ShouldResemble, expectedData)

				reverseOrder := l.Values().Slice()
				sorting.Reverse(reverseOrder)
				sorting.Sort(sorting.Compare, reverseOrder)
				c.So(entriesAsStrings(entry.NewOrderedMapFromEntries(reverseOrder)), ShouldResemble, expectedData)

				hashOrder := l.Values().Slice()
				sorting.Sort(func(a, b iface.IPFSLogEntry) (int, error) {
					return strings.Compare(a.GetHash().String(), b.GetHash().String()), nil
				}, hashOrder)
				sorting.Sort(sorting.Compare, hashOrder)
				c.So(entriesAsStrings(entry.NewOrderedMapFromEntries(hashOrder)), ShouldResemble, expectedData)

				var partialLog []iface.IPFSLogEntry
				for _, item := range l.Values().Slice() {
					if bytes.Compare(item.GetPayload(), []byte("entryC0")) != 0 {
						partialLog = append(partialLog, item)
					}
				}
				c.So(entriesAsStrings(entry.NewOrderedMapFromEntries(partialLog)), ShouldResemble, expectedData2)

				var partialLog2 []iface.IPFSLogEntry
				for _, item := range l.Values().Slice() {
					if bytes.Compare(item.GetPayload(), []byte("entryA10")) != 0 {
						partialLog2 = append(partialLog2, item)
					}
				}
				c.So(entriesAsStrings(entry.NewOrderedMapFromEntries(partialLog2)), ShouldResemble, expectedData3)

				var partialLog3 []iface.IPFSLogEntry
				for _, item := range l.Values().Slice() {
					if bytes.Compare(item.GetPayload(), []byte("entryB5")) != 0 {
						partialLog3 = append(partialLog3, item)
					}
				}
				c.So(entriesAsStrings(entry.NewOrderedMapFromEntries(partialLog3)), ShouldResemble, expectedData4)
			})

			c.Convey("sorts deterministically from random order", FailureHalts, func(c C) {
				testLog, err := logcreator.CreateLogWithSixteenEntries(ctx, ipfs, resortedIdentities)
				c.So(err, ShouldBeNil)

				l := testLog.Log
				expectedData := testLog.ExpectedData

				fetchOrder := l.Values().Slice()
				sorting.Sort(sorting.Compare, fetchOrder)
				c.So(entriesAsStrings(entry.NewOrderedMapFromEntries(fetchOrder)), ShouldResemble, expectedData)

				for i := 0; i < 1000; i++ {
					randomOrder := l.Values().Slice()
					sorting.Sort(func(a, b iface.IPFSLogEntry) (int, error) {
						return rand.Int(), nil
					}, randomOrder)
					sorting.Sort(sorting.Compare, randomOrder)

					c.So(entriesAsStrings(entry.NewOrderedMapFromEntries(randomOrder)), ShouldResemble, expectedData)
				}
			})

			c.Convey("sorts entries correctly", FailureHalts, func(c C) {
				testLog, err := logcreator.CreateLogWithHundredEntries(ctx, ipfs, resortedIdentities)
				c.So(err, ShouldBeNil)

				l := testLog.Log
				expectedData := testLog.ExpectedData

				c.So(entriesAsStrings(entry.NewOrderedMapFromEntries(l.Values().Slice())), ShouldResemble, expectedData)
			})

			c.Convey("sorts entries according to custom tiebreaker function", FailureHalts, func(c C) {
				testLog, err := logcreator.CreateLogWithSixteenEntries(ctx, ipfs, resortedIdentities)
				c.So(err, ShouldBeNil)

				firstWriteWinsLog, err := ipfslog.NewLog(ipfs, resortedIdentities[0], &ipfslog.LogOptions{ID: "X", SortFn: BadComparatorReturnsZero})
				c.So(err, ShouldBeNil)

				_, err = firstWriteWinsLog.Join(testLog.Log, -1)
				// TODO: the error is only thrown silently when calling .Values(), should we handle it properly
				//firstWriteWinsLog.Values()
				//c.So(err, ShouldNotBeNil)
			})

			c.Convey("retrieves partially joined log deterministically - single next pointer", FailureHalts, func(c C) {
				nextPointersAmount := 1

				logA, err := ipfslog.NewLog(ipfs, identities[0], &ipfslog.LogOptions{ID: "X"})
				c.So(err, ShouldBeNil)
				logB, err := ipfslog.NewLog(ipfs, identities[2], &ipfslog.LogOptions{ID: "X"})
				c.So(err, ShouldBeNil)
				log3, err := ipfslog.NewLog(ipfs, identities[3], &ipfslog.LogOptions{ID: "X"})
				c.So(err, ShouldBeNil)
				l, err := ipfslog.NewLog(ipfs, identities[1], &ipfslog.LogOptions{ID: "X"})
				c.So(err, ShouldBeNil)

				for i := 1; i <= 5; i++ {
					_, err = logA.Append(ctx, []byte(fmt.Sprintf("entryA%d", i)), nextPointersAmount)
					c.So(err, ShouldBeNil)

					_, err = logB.Append(ctx, []byte(fmt.Sprintf("entryB%d", i)), nextPointersAmount)
					c.So(err, ShouldBeNil)
				}

				_, err = log3.Join(logA, -1)
				c.So(err, ShouldBeNil)

				_, err = log3.Join(logB, -1)
				c.So(err, ShouldBeNil)

				for i := 6; i <= 10; i++ {
					_, err = logA.Append(ctx, []byte(fmt.Sprintf("entryA%d", i)), nextPointersAmount)
					c.So(err, ShouldBeNil)
				}

				_, err = l.Join(log3, -1)
				c.So(err, ShouldBeNil)

				_, err = l.Append(ctx, []byte("entryC0"), nextPointersAmount)
				c.So(err, ShouldBeNil)

				_, err = l.Join(logA, -1)
				c.So(err, ShouldBeNil)

				hash, err := l.ToMultihash(ctx)

				// First 5
				res, err := ipfslog.NewFromMultihash(ctx, ipfs, identities[1], hash, &ipfslog.LogOptions{}, &ipfslog.FetchOptions{Length: intPtr(5)})
				c.So(err, ShouldBeNil)

				first5 := []string{
					"entryA5", "entryB5", "entryC0", "entryA9", "entryA10",
				}

				c.So(entriesAsStrings(res.Values()), ShouldResemble, first5)

				// First 11
				res, err = ipfslog.NewFromMultihash(ctx, ipfs, identities[1], hash, &ipfslog.LogOptions{}, &ipfslog.FetchOptions{Length: intPtr(11)})
				c.So(err, ShouldBeNil)

				first11 := []string{
					"entryA3", "entryB3", "entryA4", "entryB4",
					"entryA5", "entryB5",
					"entryC0",
					"entryA7", "entryA8", "entryA9", "entryA10",
				}

				c.So(entriesAsStrings(res.Values()), ShouldResemble, first11)

				// All but one
				res, err = ipfslog.NewFromMultihash(ctx, ipfs, identities[1], hash, &ipfslog.LogOptions{}, &ipfslog.FetchOptions{Length: intPtr(16 - 1)})
				c.So(err, ShouldBeNil)

				all := []string{
					"entryA1" /* excl */, "entryA2", "entryB2", "entryA3", "entryB3",
					"entryA4", "entryB4", "entryA5", "entryB5",
					"entryA6",
					"entryC0", "entryA7", "entryA8", "entryA9", "entryA10",
				}

				c.So(entriesAsStrings(res.Values()), ShouldResemble, all)

			})

			c.Convey("retrieves partially joined log deterministically - multiple next pointers", FailureHalts, func(c C) {
				nextPointersAmount := 64

				logA, err := ipfslog.NewLog(ipfs, identities[0], &ipfslog.LogOptions{ID: "X"})
				c.So(err, ShouldBeNil)
				logB, err := ipfslog.NewLog(ipfs, identities[2], &ipfslog.LogOptions{ID: "X"})
				c.So(err, ShouldBeNil)
				log3, err := ipfslog.NewLog(ipfs, identities[3], &ipfslog.LogOptions{ID: "X"})
				c.So(err, ShouldBeNil)
				l, err := ipfslog.NewLog(ipfs, identities[1], &ipfslog.LogOptions{ID: "X"})
				c.So(err, ShouldBeNil)

				for i := 1; i <= 5; i++ {
					_, err = logA.Append(ctx, []byte(fmt.Sprintf("entryA%d", i)), nextPointersAmount)
					c.So(err, ShouldBeNil)

					_, err = logB.Append(ctx, []byte(fmt.Sprintf("entryB%d", i)), nextPointersAmount)
					c.So(err, ShouldBeNil)
				}

				_, err = log3.Join(logA, -1)
				c.So(err, ShouldBeNil)

				_, err = log3.Join(logB, -1)
				c.So(err, ShouldBeNil)

				for i := 6; i <= 10; i++ {
					_, err = logA.Append(ctx, []byte(fmt.Sprintf("entryA%d", i)), nextPointersAmount)
					c.So(err, ShouldBeNil)
				}

				_, err = l.Join(log3, -1)
				c.So(err, ShouldBeNil)

				_, err = l.Append(ctx, []byte("entryC0"), nextPointersAmount)
				c.So(err, ShouldBeNil)

				_, err = l.Join(logA, -1)
				c.So(err, ShouldBeNil)

				hash, err := l.ToMultihash(ctx)

				// First 5
				res, err := ipfslog.NewFromMultihash(ctx, ipfs, identities[1], hash, &ipfslog.LogOptions{}, &ipfslog.FetchOptions{Length: intPtr(5)})
				c.So(err, ShouldBeNil)

				first5 := []string{
					"entryC0", "entryA7", "entryA8", "entryA9", "entryA10",
				}

				c.So(entriesAsStrings(res.Values()), ShouldResemble, first5)

				// First 11
				res, err = ipfslog.NewFromMultihash(ctx, ipfs, identities[1], hash, &ipfslog.LogOptions{}, &ipfslog.FetchOptions{Length: intPtr(11)})
				c.So(err, ShouldBeNil)

				first11 := []string{
					"entryA1", "entryA2", "entryA3", "entryA4",
					"entryA5", "entryA6",
					"entryC0",
					"entryA7", "entryA8", "entryA9", "entryA10",
				}

				c.So(entriesAsStrings(res.Values()), ShouldResemble, first11)

				// All but one
				res, err = ipfslog.NewFromMultihash(ctx, ipfs, identities[1], hash, &ipfslog.LogOptions{}, &ipfslog.FetchOptions{Length: intPtr(16 - 1)})
				c.So(err, ShouldBeNil)

				all := []string{
					"entryA1" /* excl */, "entryA2", "entryB2", "entryA3", "entryB3",
					"entryA4", "entryB4", "entryA5", "entryB5",
					"entryA6",
					"entryC0", "entryA7", "entryA8", "entryA9", "entryA10",
				}

				c.So(entriesAsStrings(res.Values()), ShouldResemble, all)
			})

			c.Convey("throws an error if ipfs is not defined", FailureHalts, func(c C) {
				_, err := ipfslog.NewFromEntry(ctx, nil, identities[0], []iface.IPFSLogEntry{}, &ipfslog.LogOptions{ID: "X"}, &entry.FetchOptions{})
				c.So(err, ShouldNotBeNil)
				c.So(err.Error(), ShouldContainSubstring, "ipfs instance not defined")
			})

			c.Convey("fetches a log", FailureHalts, func(c C) {
				const amount = 100

				ts := time.Now().UnixNano() / 1000
				log1, err := ipfslog.NewLog(ipfs, identities[0], &ipfslog.LogOptions{ID: "X"})
				c.So(err, ShouldBeNil)

				log2, err := ipfslog.NewLog(ipfs, identities[1], &ipfslog.LogOptions{ID: "X"})
				c.So(err, ShouldBeNil)

				log3, err := ipfslog.NewLog(ipfs, identities[2], &ipfslog.LogOptions{ID: "X"})
				c.So(err, ShouldBeNil)

				var items1 []iface.IPFSLogEntry
				var items2 []iface.IPFSLogEntry
				var items3 []iface.IPFSLogEntry

				for i := 1; i <= amount; i++ {
					var nexts []cid.Cid
					prev1 := lastEntry(items1)
					prev2 := lastEntry(items2)
					prev3 := lastEntry(items3)

					if prev1 != nil {
						nexts = []cid.Cid{prev1.GetHash()}
					}

					n1, err := entry.CreateEntry(ctx, ipfs, log1.Identity, &entry.Entry{LogID: log1.ID, Payload: []byte(fmt.Sprintf("entryA%d-%d", i, ts)), Next: nexts}, log1.Clock)
					c.So(err, ShouldBeNil)

					nexts = []cid.Cid{n1.Hash}
					if prev2 != nil {
						nexts = []cid.Cid{prev2.GetHash(), n1.Hash}
					}

					n2, err := entry.CreateEntry(ctx, ipfs, log2.Identity, &entry.Entry{LogID: log2.ID, Payload: []byte(fmt.Sprintf("entryB%d-%d", i, ts)), Next: nexts}, log2.Clock)
					c.So(err, ShouldBeNil)

					nexts = []cid.Cid{n1.Hash, n2.Hash}
					if prev2 != nil {
						nexts = []cid.Cid{prev3.GetHash(), n1.Hash, n2.Hash}
					}

					n3, err := entry.CreateEntry(ctx, ipfs, log3.Identity, &entry.Entry{LogID: log3.ID, Payload: []byte(fmt.Sprintf("entryC%d-%d", i, ts)), Next: nexts}, log3.Clock)
					c.So(err, ShouldBeNil)

					log1.Clock.Tick()
					log2.Clock.Tick()
					log3.Clock.Tick()
					log1.Clock.Merge(log2.Clock)
					log1.Clock.Merge(log3.Clock)
					log2.Clock.Merge(log1.Clock)
					log2.Clock.Merge(log3.Clock)
					log3.Clock.Merge(log1.Clock)
					log3.Clock.Merge(log2.Clock)
					items1 = append(items1, n1)
					items2 = append(items2, n2)
					items3 = append(items3, n3)
				}

				c.Convey("returns all entries - no excluded entries", FailureHalts, func(c C) {
					a, err := ipfslog.NewFromEntry(ctx, ipfs, identities[0], []iface.IPFSLogEntry{lastEntry(items1)}, &ipfslog.LogOptions{}, &entry.FetchOptions{Length: intPtr(-1)})
					c.So(err, ShouldBeNil)

					c.So(a.Values().Len(), ShouldEqual, amount)
					c.So(a.Values().At(0).GetHash().String(), ShouldEqual, items1[0].GetHash().String())
				})

				c.Convey("returns all entries - including excluded entries", FailureHalts, func(c C) {
					// One entry
					a, err := ipfslog.NewFromEntry(ctx, ipfs, identities[0], []iface.IPFSLogEntry{lastEntry(items1)}, &ipfslog.LogOptions{}, &entry.FetchOptions{Exclude: []iface.IPFSLogEntry{items1[0]}, Length: intPtr(-1)})
					c.So(err, ShouldBeNil)

					c.So(a.Values().Len(), ShouldEqual, amount)
					c.So(a.Values().At(0).GetHash().String(), ShouldEqual, items1[0].GetHash().String())

					// All entries
					b, err := ipfslog.NewFromEntry(ctx, ipfs, identities[0], []iface.IPFSLogEntry{lastEntry(items1)}, &ipfslog.LogOptions{}, &entry.FetchOptions{Exclude: items1, Length: intPtr(-1)})
					c.So(err, ShouldBeNil)

					c.So(b.Values().Len(), ShouldEqual, amount)
					c.So(b.Values().At(0).GetHash().String(), ShouldEqual, items1[0].GetHash().String())
				})
			})
		})
	})
}
