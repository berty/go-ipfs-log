package test

import (
	"context"
	"fmt"
	"reflect"
	"testing"
	"time"

	"github.com/berty/go-ipfs-log/entry"
	idp "github.com/berty/go-ipfs-log/identityprovider"
	"github.com/berty/go-ipfs-log/io"
	ks "github.com/berty/go-ipfs-log/keystore"
	"github.com/berty/go-ipfs-log/log"
	"github.com/berty/go-ipfs-log/utils/lamportclock"
	"github.com/ipfs/go-cid"
	dssync "github.com/ipfs/go-datastore/sync"

	. "github.com/smartystreets/goconvey/convey"
)

func TestLogJoin(t *testing.T) {
	_, cancel := context.WithTimeout(context.Background(), time.Second*10)
	defer cancel()

	ipfs := io.NewMemoryServices()

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
			ID: fmt.Sprintf("user%c", char),
			Type: "orbitdb",
		})

		if err != nil {
			panic(err)
		}

		identities = append(identities, identity)
	}

	Convey("Log - Join", t, FailureHalts, func(c C) {
		c.Convey("join", FailureHalts, func(c C) {
			var logs []*log.Log

			for i := 0; i < 4; i++ {
				l, err := log.NewLog(ipfs, identities[i], &log.NewLogOptions{ID: "X"})
				c.So(err, ShouldBeNil)

				logs = append(logs, l)
			}

			c.Convey("joins logs", FailureHalts, func() {
				var items [3][]*entry.Entry
				var prev [3]*entry.Entry
				var curr [3]*entry.Entry
				var err error

				curr[0], err = entry.CreateEntry(ipfs, identities[0], &entry.Entry{Payload: []byte("EntryA0"), LogID: "X"}, nil)
				c.So(err, ShouldBeNil)
				curr[1], err = entry.CreateEntry(ipfs, identities[1], &entry.Entry{Payload: []byte("EntryB0"), LogID: "X", Next: []cid.Cid{curr[0].Hash}}, nil)
				c.So(err, ShouldBeNil)
				curr[2], err = entry.CreateEntry(ipfs, identities[2], &entry.Entry{Payload: []byte("EntryC0"), LogID: "X", Next: []cid.Cid{curr[0].Hash, curr[1].Hash}}, nil)
				c.So(err, ShouldBeNil)
				for i := 0; i < 100; i++ {
					if i > 0 {
						for j := 0; j < 3; j++ {
							prev[j] = items[j][len(items[j])-1]
						}
						curr[0], err = entry.CreateEntry(ipfs, identities[0], &entry.Entry{Payload: []byte(fmt.Sprintf("EntryA%d", i)), LogID: "X", Next: []cid.Cid{prev[0].Hash}}, nil)
						c.So(err, ShouldBeNil)
						curr[1], err = entry.CreateEntry(ipfs, identities[1], &entry.Entry{Payload: []byte(fmt.Sprintf("EntryB%d", i)), LogID: "X", Next: []cid.Cid{prev[1].Hash, curr[0].Hash}}, nil)
						c.So(err, ShouldBeNil)
						curr[2], err = entry.CreateEntry(ipfs, identities[2], &entry.Entry{Payload: []byte(fmt.Sprintf("EntryC%d", i)), LogID: "X", Next: []cid.Cid{prev[2].Hash, curr[0].Hash, curr[1].Hash}}, nil)
						c.So(err, ShouldBeNil)
					}

					for j := 0; j < 3; j++ {
						items[j] = append(items[j], curr[j])
					}
				}

				// Here we're creating a log from entries signed by A and B
				// but we accept entries from C too
				logA, err := log.NewFromEntry(ipfs, identities[2], []*entry.Entry{items[1][len(items[1])-1]}, &log.NewLogOptions{}, &entry.FetchOptions{})
				c.So(err, ShouldBeNil)
				// Here we're creating a log from entries signed by peer A, B and C
				// "logA" accepts entries from peer C so we can join logs A and B
				logB, err := log.NewFromEntry(ipfs, identities[2], []*entry.Entry{items[2][len(items[2])-1]}, &log.NewLogOptions{}, &entry.FetchOptions{})
				c.So(err, ShouldBeNil)

				c.So(entry.EntriesAsStrings(logA.Values().Slice()), ShouldResemble, entry.EntriesAsStrings(append(items[0], items[1]...)))
				c.So(entry.EntriesAsStrings(logB.Values().Slice()), ShouldResemble, entry.EntriesAsStrings(append(items[0], append(items[1], items[2]...)...)))

				_, err = logA.Join(logB, -1)
				c.So(err, ShouldBeNil)

				c.So(entry.EntriesAsStrings(logA.Values().Slice()), ShouldResemble, entry.EntriesAsStrings(append(items[0], append(items[1], items[2]...)...)))

				// The last entry, 'entryC100', should be the only head
				// (it points to entryB100, entryB100 and entryC99)
				c.So(len(log.FindHeads(logA.Entries)), ShouldEqual, 1)
			})

			c.Convey("returns error if log parameter is not defined", FailureHalts, func() {
				_, err := logs[0].Join(nil, -1)
				c.So(err.Error(), ShouldEqual, "log to join is not defined")
			})

			c.Convey("joins only unique items", FailureHalts, func() {
				_, err := logs[0].Append([]byte("helloA1"), 1)
				c.So(err, ShouldBeNil)
				_, err = logs[0].Append([]byte("helloA2"), 1)
				c.So(err, ShouldBeNil)
				_, err = logs[1].Append([]byte("helloB1"), 1)
				c.So(err, ShouldBeNil)
				_, err = logs[1].Append([]byte("helloB2"), 1)
				c.So(err, ShouldBeNil)

				_, err = logs[0].Join(logs[1], -1)
				c.So(err, ShouldBeNil)
				_, err = logs[0].Join(logs[1], -1)
				c.So(err, ShouldBeNil)

				c.So(logs[0].Values().Len(), ShouldEqual, 4)

				expected := []string{"helloA1", "helloB1", "helloA2", "helloB2"}
				var result []string

				for _, v := range logs[0].Values().Keys() {
					result = append(result, string(logs[0].Values().UnsafeGet(v).Payload))
				}

				c.So(expected, ShouldResemble, result)
				c.So(len(getLastEntry(logs[0].Values()).Next), ShouldEqual, 1)
			})

			c.Convey("joins logs two ways", FailureHalts, func() {
				_, err := logs[0].Append([]byte("helloA1"), 1)
				c.So(err, ShouldBeNil)
				_, err = logs[0].Append([]byte("helloA2"), 1)
				c.So(err, ShouldBeNil)
				_, err = logs[1].Append([]byte("helloB1"), 1)
				c.So(err, ShouldBeNil)
				_, err = logs[1].Append([]byte("helloB2"), 1)
				c.So(err, ShouldBeNil)

				_, err = logs[0].Join(logs[1], -1)
				c.So(err, ShouldBeNil)
				_, err = logs[1].Join(logs[0], -1)
				c.So(err, ShouldBeNil)

				var hashes [2][]cid.Cid
				var payloads [2][][]byte
				expected := [][]byte{[]byte("helloA1"), []byte("helloB1"), []byte("helloA2"), []byte("helloB2")}

				for i := 0; i < 2; i++ {
					values := logs[i].Values()
					keys := values.Keys()
					for _, k := range keys {
						v := values.UnsafeGet(k)
						hashes[i] = append(hashes[i], v.Hash)
						payloads[i] = append(payloads[i], v.Payload)
					}
				}

				c.So(reflect.DeepEqual(hashes[0], hashes[1]), ShouldBeTrue)
				// TODO: Add fixed key and enable the following tests
				_ = expected
				//c.So(payloads[0], ShouldResemble, expected)
				//c.So(payloads[1], ShouldResemble, expected)
			})

			c.Convey("joins logs twice", FailureHalts, func() {
				_, err := logs[0].Append([]byte("helloA1"), 1)
				c.So(err, ShouldBeNil)
				_, err = logs[1].Append([]byte("helloB1"), 1)
				c.So(err, ShouldBeNil)
				_, err = logs[1].Join(logs[0], -1)
				c.So(err, ShouldBeNil)

				_, err = logs[0].Append([]byte("helloA2"), 1)
				c.So(err, ShouldBeNil)
				_, err = logs[1].Append([]byte("helloB2"), 1)
				c.So(err, ShouldBeNil)
				_, err = logs[1].Join(logs[0], -1)
				c.So(err, ShouldBeNil)

				c.So(logs[1].Values().Len(), ShouldEqual, 4)

				expected := []string{"helloA1", "helloB1", "helloA2", "helloB2"}
				var result []string

				for _, v := range logs[1].Values().Keys() {
					result = append(result, string(logs[1].Values().UnsafeGet(v).Payload))
				}

				c.So(expected, ShouldResemble, result)
			})

			c.Convey("joins 2 logs two ways", FailureHalts, func() {
				_, err := logs[0].Append([]byte("helloA1"), 1)
				c.So(err, ShouldBeNil)
				_, err = logs[1].Append([]byte("helloB1"), 1)
				c.So(err, ShouldBeNil)
				_, err = logs[1].Join(logs[0], -1)
				c.So(err, ShouldBeNil)
				_, err = logs[0].Join(logs[1], -1)
				c.So(err, ShouldBeNil)

				_, err = logs[0].Append([]byte("helloA2"), 1)
				c.So(err, ShouldBeNil)
				_, err = logs[1].Append([]byte("helloB2"), 1)
				c.So(err, ShouldBeNil)
				_, err = logs[1].Join(logs[0], -1)
				c.So(err, ShouldBeNil)

				c.So(logs[1].Values().Len(), ShouldEqual, 4)

				expected := []string{"helloA1", "helloB1", "helloA2", "helloB2"}
				var result []string

				for _, v := range logs[1].Values().Keys() {
					result = append(result, string(logs[1].Values().UnsafeGet(v).Payload))
				}

				c.So(expected, ShouldResemble, result)
			})

			c.Convey("joins 2 logs two ways and has the right heads at every step", FailureHalts, func() {
				_, err := logs[0].Append([]byte("helloA1"), 1)
				c.So(err, ShouldBeNil)
				c.So(len(log.FindHeads(logs[0].Entries)), ShouldEqual, 1)
				c.So(string(log.FindHeads(logs[0].Entries)[0].Payload), ShouldEqual, "helloA1")

				_, err = logs[1].Append([]byte("helloB1"), 1)
				c.So(err, ShouldBeNil)
				c.So(len(log.FindHeads(logs[1].Entries)), ShouldEqual, 1)
				c.So(string(log.FindHeads(logs[1].Entries)[0].Payload), ShouldEqual, "helloB1")

				_, err = logs[1].Join(logs[0], -1)
				c.So(err, ShouldBeNil)
				c.So(len(log.FindHeads(logs[1].Entries)), ShouldEqual, 2)
				c.So(string(log.FindHeads(logs[1].Entries)[0].Payload), ShouldEqual, "helloB1")
				c.So(string(log.FindHeads(logs[1].Entries)[1].Payload), ShouldEqual, "helloA1")

				_, err = logs[0].Join(logs[1], -1)
				c.So(err, ShouldBeNil)
				c.So(len(log.FindHeads(logs[0].Entries)), ShouldEqual, 2)
				c.So(string(log.FindHeads(logs[0].Entries)[0].Payload), ShouldEqual, "helloB1")
				c.So(string(log.FindHeads(logs[0].Entries)[1].Payload), ShouldEqual, "helloA1")

				_, err = logs[0].Append([]byte("helloA2"), 1)
				c.So(err, ShouldBeNil)
				c.So(len(log.FindHeads(logs[0].Entries)), ShouldEqual, 1)
				c.So(string(log.FindHeads(logs[0].Entries)[0].Payload), ShouldEqual, "helloA2")

				_, err = logs[1].Append([]byte("helloB2"), 1)
				c.So(err, ShouldBeNil)
				c.So(len(log.FindHeads(logs[1].Entries)), ShouldEqual, 1)
				c.So(string(log.FindHeads(logs[1].Entries)[0].Payload), ShouldEqual, "helloB1")

				_, err = logs[1].Join(logs[0], -1)
				c.So(err, ShouldBeNil)
				c.So(len(log.FindHeads(logs[1].Entries)), ShouldEqual, 2)
				c.So(string(log.FindHeads(logs[1].Entries)[0].Payload), ShouldEqual, "helloB2")
				c.So(string(log.FindHeads(logs[1].Entries)[1].Payload), ShouldEqual, "helloA2")
			})

			c.Convey("joins 4 logs to one", FailureHalts, func() {
				// order determined by identity's publicKey
				_, err := logs[0].Append([]byte("helloA1"), 1)
				c.So(err, ShouldBeNil)
				_, err = logs[0].Append([]byte("helloA2"), 1)
				c.So(err, ShouldBeNil)

				_, err = logs[2].Append([]byte("helloB1"), 1)
				c.So(err, ShouldBeNil)
				_, err = logs[2].Append([]byte("helloB2"), 1)
				c.So(err, ShouldBeNil)

				_, err = logs[1].Append([]byte("helloC1"), 1)
				c.So(err, ShouldBeNil)
				_, err = logs[1].Append([]byte("helloC2"), 1)
				c.So(err, ShouldBeNil)

				_, err = logs[3].Append([]byte("helloD1"), 1)
				c.So(err, ShouldBeNil)
				_, err = logs[3].Append([]byte("helloD2"), 1)
				c.So(err, ShouldBeNil)

				_, err = logs[0].Join(logs[1], -1)
				c.So(err, ShouldBeNil)
				_, err = logs[0].Join(logs[2], -1)
				c.So(err, ShouldBeNil)
				_, err = logs[0].Join(logs[3], -1)
				c.So(err, ShouldBeNil)

				expected := []string{
					"helloA1",
					"helloB1",
					"helloC1",
					"helloD1",
					"helloA2",
					"helloB2",
					"helloC2",
					"helloD2",
				}

				c.So(logs[0].Values().Len(), ShouldEqual, 8)

				var result []string

				for _, v := range logs[0].Values().Keys() {
					result = append(result, string(logs[0].Values().UnsafeGet(v).Payload))
				}

				c.So(expected, ShouldResemble, result)
			})

			c.Convey("joins 4 logs to one is commutative", FailureHalts, func() {
				_, err := logs[0].Append([]byte("helloA1"), 1)
				c.So(err, ShouldBeNil)
				_, err = logs[0].Append([]byte("helloA2"), 1)
				c.So(err, ShouldBeNil)

				_, err = logs[1].Append([]byte("helloB1"), 1)
				c.So(err, ShouldBeNil)
				_, err = logs[1].Append([]byte("helloB2"), 1)
				c.So(err, ShouldBeNil)

				_, err = logs[2].Append([]byte("helloC1"), 1)
				c.So(err, ShouldBeNil)
				_, err = logs[2].Append([]byte("helloC2"), 1)
				c.So(err, ShouldBeNil)

				_, err = logs[3].Append([]byte("helloD1"), 1)
				c.So(err, ShouldBeNil)
				_, err = logs[3].Append([]byte("helloD2"), 1)
				c.So(err, ShouldBeNil)

				_, err = logs[0].Join(logs[1], -1)
				c.So(err, ShouldBeNil)
				_, err = logs[0].Join(logs[2], -1)
				c.So(err, ShouldBeNil)
				_, err = logs[0].Join(logs[3], -1)
				c.So(err, ShouldBeNil)

				_, err = logs[1].Join(logs[0], -1)
				c.So(err, ShouldBeNil)
				_, err = logs[1].Join(logs[2], -1)
				c.So(err, ShouldBeNil)
				_, err = logs[1].Join(logs[3], -1)
				c.So(err, ShouldBeNil)

				c.So(logs[0].Values().Len(), ShouldEqual, 8)

				var payloads [2][]string

				for i := 0; i < 2; i++ {
					for _, v := range logs[i].Values().Keys() {
						payloads[i] = append(payloads[i], string(logs[i].Values().UnsafeGet(v).Payload))
					}
				}

				c.So(payloads[0], ShouldResemble, payloads[1])
			})

			c.Convey("joins logs and updates clocks", FailureHalts, func() {
				_, err := logs[0].Append([]byte("helloA1"), 1)
				c.So(err, ShouldBeNil)
				_, err = logs[1].Append([]byte("helloB1"), 1)
				c.So(err, ShouldBeNil)
				_, err = logs[1].Join(logs[0], -1)
				c.So(err, ShouldBeNil)
				_, err = logs[0].Append([]byte("helloA2"), 1)
				c.So(err, ShouldBeNil)
				_, err = logs[1].Append([]byte("helloB2"), 1)
				c.So(err, ShouldBeNil)

				c.So(logs[0].Clock.ID, ShouldResemble, identities[0].PublicKey)
				c.So(logs[1].Clock.ID, ShouldResemble, identities[1].PublicKey)
				c.So(logs[0].Clock.Time, ShouldEqual, 2)
				c.So(logs[1].Clock.Time, ShouldEqual, 2)

				_, err = logs[2].Join(logs[0], -1)
				c.So(err, ShouldBeNil)
				c.So(logs[2].ID, ShouldEqual, "X")
				c.So(logs[2].Clock.ID, ShouldResemble, identities[2].PublicKey)
				c.So(logs[2].Clock.Time, ShouldEqual, 2)

				_, err = logs[2].Append([]byte("helloC1"), 1)
				c.So(err, ShouldBeNil)
				_, err = logs[2].Append([]byte("helloC2"), 1)
				c.So(err, ShouldBeNil)
				_, err = logs[0].Join(logs[2], -1)
				c.So(err, ShouldBeNil)
				_, err = logs[0].Join(logs[1], -1)
				c.So(err, ShouldBeNil)
				_, err = logs[3].Append([]byte("helloD1"), 1)
				c.So(err, ShouldBeNil)
				_, err = logs[3].Append([]byte("helloD2"), 1)
				c.So(err, ShouldBeNil)
				_, err = logs[3].Join(logs[1], -1)
				c.So(err, ShouldBeNil)
				_, err = logs[3].Join(logs[0], -1)
				c.So(err, ShouldBeNil)
				_, err = logs[3].Join(logs[2], -1)
				c.So(err, ShouldBeNil)
				_, err = logs[3].Append([]byte("helloD3"), 1)
				c.So(err, ShouldBeNil)
				_, err = logs[3].Append([]byte("helloD4"), 1)
				c.So(err, ShouldBeNil)

				_, err = logs[0].Join(logs[3], -1)
				c.So(err, ShouldBeNil)
				_, err = logs[3].Join(logs[0], -1)
				c.So(err, ShouldBeNil)
				_, err = logs[3].Append([]byte("helloD5"), 1)
				c.So(err, ShouldBeNil)
				_, err = logs[0].Append([]byte("helloA5"), 1)
				c.So(err, ShouldBeNil)
				_, err = logs[3].Join(logs[0], -1)
				c.So(err, ShouldBeNil)
				c.So(logs[3].Clock.ID, ShouldResemble, identities[3].PublicKey)
				c.So(logs[3].Clock.Time, ShouldEqual, 7)

				_, err = logs[3].Append([]byte("helloD6"), 1)
				c.So(err, ShouldBeNil)
				c.So(logs[3].Clock.Time, ShouldEqual, 8)

				expected := []entry.Entry{
					entry.Entry{Payload: []byte("helloA1"), LogID: "X", Clock: &lamportclock.LamportClock{ID: identities[0].PublicKey, Time: 1}},
					entry.Entry{Payload: []byte("helloB1"), LogID: "X", Clock: &lamportclock.LamportClock{ID: identities[1].PublicKey, Time: 1}},
					entry.Entry{Payload: []byte("helloD1"), LogID: "X", Clock: &lamportclock.LamportClock{ID: identities[3].PublicKey, Time: 1}},
					entry.Entry{Payload: []byte("helloA2"), LogID: "X", Clock: &lamportclock.LamportClock{ID: identities[0].PublicKey, Time: 2}},
					entry.Entry{Payload: []byte("helloB2"), LogID: "X", Clock: &lamportclock.LamportClock{ID: identities[1].PublicKey, Time: 2}},
					entry.Entry{Payload: []byte("helloD2"), LogID: "X", Clock: &lamportclock.LamportClock{ID: identities[3].PublicKey, Time: 2}},
					entry.Entry{Payload: []byte("helloC1"), LogID: "X", Clock: &lamportclock.LamportClock{ID: identities[2].PublicKey, Time: 3}},
					entry.Entry{Payload: []byte("helloC2"), LogID: "X", Clock: &lamportclock.LamportClock{ID: identities[2].PublicKey, Time: 4}},
					entry.Entry{Payload: []byte("helloD3"), LogID: "X", Clock: &lamportclock.LamportClock{ID: identities[3].PublicKey, Time: 5}},
					entry.Entry{Payload: []byte("helloD4"), LogID: "X", Clock: &lamportclock.LamportClock{ID: identities[3].PublicKey, Time: 6}},
					entry.Entry{Payload: []byte("helloA5"), LogID: "X", Clock: &lamportclock.LamportClock{ID: identities[0].PublicKey, Time: 7}},
					entry.Entry{Payload: []byte("helloD5"), LogID: "X", Clock: &lamportclock.LamportClock{ID: identities[3].PublicKey, Time: 7}},
					entry.Entry{Payload: []byte("helloD6"), LogID: "X", Clock: &lamportclock.LamportClock{ID: identities[3].PublicKey, Time: 8}},
				}

				c.So(logs[3].Values().Len(), ShouldEqual, 13)

				var result []entry.Entry

				for _, v := range logs[3].Values().Keys() {
					e, exist := logs[3].Values().Get(v)
					c.So(exist, ShouldBeTrue)
					result = append(result, entry.Entry{Payload: e.Payload, LogID: e.LogID, Clock: e.Clock})
				}

				c.So(reflect.DeepEqual(result, expected), ShouldBeTrue)
			})

			c.Convey("joins logs from 4 logs", FailureHalts, func() {
				_, err := logs[0].Append([]byte("helloA1"), 1)
				c.So(err, ShouldBeNil)
				_, err = logs[0].Join(logs[1], -1)
				c.So(err, ShouldBeNil)
				_, err = logs[1].Append([]byte("helloB1"), 1)
				c.So(err, ShouldBeNil)
				_, err = logs[1].Join(logs[0], -1)
				c.So(err, ShouldBeNil)
				_, err = logs[0].Append([]byte("helloA2"), 1)
				c.So(err, ShouldBeNil)
				_, err = logs[1].Append([]byte("helloB2"), 1)
				c.So(err, ShouldBeNil)

				_, err = logs[0].Join(logs[2], -1)
				c.So(err, ShouldBeNil)
				c.So(logs[0].ID, ShouldEqual, "X")
				c.So(logs[0].Clock.ID, ShouldResemble, identities[0].PublicKey)
				c.So(logs[0].Clock.Time, ShouldEqual, 2)

				_, err = logs[2].Join(logs[0], -1)
				c.So(err, ShouldBeNil)
				c.So(logs[2].ID, ShouldEqual, "X")
				c.So(logs[2].Clock.ID, ShouldResemble, identities[2].PublicKey)
				c.So(logs[2].Clock.Time, ShouldEqual, 2)

				_, err = logs[2].Append([]byte("helloC1"), 1)
				c.So(err, ShouldBeNil)
				_, err = logs[2].Append([]byte("helloC2"), 1)
				c.So(err, ShouldBeNil)
				_, err = logs[0].Join(logs[2], -1)
				c.So(err, ShouldBeNil)
				_, err = logs[0].Join(logs[1], -1)
				c.So(err, ShouldBeNil)
				_, err = logs[3].Append([]byte("helloD1"), 1)
				c.So(err, ShouldBeNil)
				_, err = logs[3].Append([]byte("helloD2"), 1)
				c.So(err, ShouldBeNil)
				_, err = logs[3].Join(logs[1], -1)
				c.So(err, ShouldBeNil)
				_, err = logs[3].Join(logs[0], -1)
				c.So(err, ShouldBeNil)
				_, err = logs[3].Join(logs[2], -1)
				c.So(err, ShouldBeNil)
				_, err = logs[3].Append([]byte("helloD3"), 1)
				c.So(err, ShouldBeNil)
				_, err = logs[3].Append([]byte("helloD4"), 1)
				c.So(err, ShouldBeNil)

				c.So(logs[3].Clock.ID, ShouldResemble, identities[3].PublicKey)
				c.So(logs[3].Clock.Time, ShouldEqual, 6)

				expected := [][]byte{
					[]byte("helloA1"),
					[]byte("helloB1"),
					[]byte("helloD1"),
					[]byte("helloA2"),
					[]byte("helloB2"),
					[]byte("helloD2"),
					[]byte("helloC1"),
					[]byte("helloC2"),
					[]byte("helloD3"),
					[]byte("helloD4"),
				}

				c.So(logs[3].Values().Len(), ShouldEqual, 10)

				var result [][]byte

				for _, v := range logs[3].Values().Keys() {
					result = append(result, logs[3].Values().UnsafeGet(v).Payload)
				}

				c.So(reflect.DeepEqual(expected, result), ShouldBeTrue)
			})

			c.Convey("joins only specified amount of entries - one entry", FailureHalts, func() {
				_, err := logs[0].Append([]byte("helloA1"), 1)
				c.So(err, ShouldBeNil)
				_, err = logs[0].Append([]byte("helloA2"), 1)
				c.So(err, ShouldBeNil)
				_, err = logs[1].Append([]byte("helloB1"), 1)
				c.So(err, ShouldBeNil)
				_, err = logs[1].Append([]byte("helloB2"), 1)
				c.So(err, ShouldBeNil)

				_, err = logs[0].Join(logs[1], 1)
				c.So(err, ShouldBeNil)

				expected := [][]byte{[]byte("helloB2")}

				c.So(logs[0].Values().Len(), ShouldEqual, 1)

				var result [][]byte
				var key string

				for _, v := range logs[0].Values().Keys() {
					result = append(result, logs[0].Values().UnsafeGet(v).Payload)
					key = v
				}

				c.So(reflect.DeepEqual(expected, result), ShouldBeTrue)
				c.So(len(logs[0].Values().UnsafeGet(key).Next), ShouldEqual, 1)
			})

			c.Convey("joins only specified amount of entries - two entries", FailureHalts, func() {
				_, err := logs[0].Append([]byte("helloA1"), 1)
				c.So(err, ShouldBeNil)
				_, err = logs[0].Append([]byte("helloA2"), 1)
				c.So(err, ShouldBeNil)
				_, err = logs[1].Append([]byte("helloB1"), 1)
				c.So(err, ShouldBeNil)
				_, err = logs[1].Append([]byte("helloB2"), 1)
				c.So(err, ShouldBeNil)

				_, err = logs[0].Join(logs[1], 2)
				c.So(err, ShouldBeNil)

				expected := [][]byte{[]byte("helloA2"), []byte("helloB2")}

				c.So(logs[0].Values().Len(), ShouldEqual, 2)

				var result [][]byte
				var key string

				for _, v := range logs[0].Values().Keys() {
					result = append(result, logs[0].Values().UnsafeGet(v).Payload)
					key = v
				}

				c.So(reflect.DeepEqual(expected, result), ShouldBeTrue)
				c.So(len(logs[0].Values().UnsafeGet(key).Next), ShouldEqual, 1)
			})

			c.Convey("joins only specified amount of entries - three entries", FailureHalts, func() {
				_, err := logs[0].Append([]byte("helloA1"), 1)
				c.So(err, ShouldBeNil)
				_, err = logs[0].Append([]byte("helloA2"), 1)
				c.So(err, ShouldBeNil)
				_, err = logs[1].Append([]byte("helloB1"), 1)
				c.So(err, ShouldBeNil)
				_, err = logs[1].Append([]byte("helloB2"), 1)
				c.So(err, ShouldBeNil)

				_, err = logs[0].Join(logs[1], 3)
				c.So(err, ShouldBeNil)

				expected := [][]byte{[]byte("helloB1"), []byte("helloA2"), []byte("helloB2")}

				c.So(logs[0].Values().Len(), ShouldEqual, 3)

				var result [][]byte
				var key string

				for _, v := range logs[0].Values().Keys() {
					result = append(result, logs[0].Values().UnsafeGet(v).Payload)
					key = v
				}

				c.So(reflect.DeepEqual(expected, result), ShouldBeTrue)
				c.So(len(logs[0].Values().UnsafeGet(key).Next), ShouldEqual, 1)
			})

			c.Convey("joins only specified amount of entries - (all) four entries", FailureHalts, func() {
				_, err := logs[0].Append([]byte("helloA1"), 1)
				c.So(err, ShouldBeNil)
				_, err = logs[0].Append([]byte("helloA2"), 1)
				c.So(err, ShouldBeNil)
				_, err = logs[1].Append([]byte("helloB1"), 1)
				c.So(err, ShouldBeNil)
				_, err = logs[1].Append([]byte("helloB2"), 1)
				c.So(err, ShouldBeNil)

				_, err = logs[0].Join(logs[1], 4)
				c.So(err, ShouldBeNil)

				expected := [][]byte{[]byte("helloA1"), []byte("helloB1"), []byte("helloA2"), []byte("helloB2")}

				c.So(logs[0].Values().Len(), ShouldEqual, 4)

				var result [][]byte
				var key string

				for _, v := range logs[0].Values().Keys() {
					result = append(result, logs[0].Values().UnsafeGet(v).Payload)
					key = v
				}

				c.So(reflect.DeepEqual(expected, result), ShouldBeTrue)
				c.So(len(logs[0].Values().UnsafeGet(key).Next), ShouldEqual, 1)
			})
		})
	})
}
