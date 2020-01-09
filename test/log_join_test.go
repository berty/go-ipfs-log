package test

import (
	"context"
	"fmt"
	"reflect"
	"testing"
	"time"

	"berty.tech/go-ipfs-log/iface"

	ipfslog "berty.tech/go-ipfs-log"
	"berty.tech/go-ipfs-log/entry"
	"berty.tech/go-ipfs-log/errmsg"
	idp "berty.tech/go-ipfs-log/identity"
	ks "berty.tech/go-ipfs-log/keystore"
	"github.com/ipfs/go-cid"
	dssync "github.com/ipfs/go-datastore/sync"
	. "github.com/smartystreets/goconvey/convey"
)

func TestLogJoin(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*10)
	defer cancel()

	ipfs := NewMemoryServices()

	datastore := dssync.MutexWrap(NewIdentityDataStore())
	keystore, err := ks.New(datastore)
	if err != nil {
		panic(err)
	}

	var identities [4]*idp.Identity

	for i, char := range []rune{'C', 'B', 'D', 'A'} {

		identity, err := idp.CreateIdentity(keystore, fmt.Sprintf("user%c", char))
		if err != nil {
			panic(err)
		}

		identities[i] = identity
	}

	Convey("IPFSLog - Join", t, FailureHalts, func(c C) {
		c.Convey("join", FailureHalts, func(c C) {
			var logs []*ipfslog.IPFSLog

			for i := 0; i < 4; i++ {
				l, err := ipfslog.NewLog(ipfs, identities[i], &ipfslog.LogOptions{ID: "X"})
				c.So(err, ShouldBeNil)

				logs = append(logs, l)
			}

			c.Convey("joins logs", FailureHalts, func() {
				var items [3][]*entry.Entry
				var prev [3]*entry.Entry
				var curr [3]*entry.Entry
				var err error

				curr[0], err = entry.CreateEntry(ctx, ipfs, identities[0], &entry.Entry{Payload: []byte("entryA1"), LogID: "X"}, nil)
				c.So(err, ShouldBeNil)
				curr[1], err = entry.CreateEntry(ctx, ipfs, identities[1], &entry.Entry{Payload: []byte("entryB1"), LogID: "X", Next: []cid.Cid{curr[0].Hash}}, nil)
				c.So(err, ShouldBeNil)
				curr[2], err = entry.CreateEntry(ctx, ipfs, identities[2], &entry.Entry{Payload: []byte("entryC1"), LogID: "X", Next: []cid.Cid{curr[0].Hash, curr[1].Hash}}, nil)
				c.So(err, ShouldBeNil)
				for i := 1; i <= 100; i++ {
					if i > 1 {
						for j := 0; j < 3; j++ {
							prev[j] = items[j][len(items[j])-1]
						}
						curr[0], err = entry.CreateEntry(ctx, ipfs, identities[0], &entry.Entry{Payload: []byte(fmt.Sprintf("entryA%d", i)), LogID: "X", Next: []cid.Cid{prev[0].Hash}}, nil)
						c.So(err, ShouldBeNil)
						curr[1], err = entry.CreateEntry(ctx, ipfs, identities[1], &entry.Entry{Payload: []byte(fmt.Sprintf("entryB%d", i)), LogID: "X", Next: []cid.Cid{prev[1].Hash, curr[0].Hash}}, nil)
						c.So(err, ShouldBeNil)
						curr[2], err = entry.CreateEntry(ctx, ipfs, identities[2], &entry.Entry{Payload: []byte(fmt.Sprintf("entryC%d", i)), LogID: "X", Next: []cid.Cid{prev[2].Hash, curr[0].Hash, curr[1].Hash}}, nil)
						c.So(err, ShouldBeNil)
					}

					for j := 0; j < 3; j++ {
						items[j] = append(items[j], curr[j])
					}
				}

				// Here we're creating a log from entries signed by A and B
				// but we accept entries from C too
				logA, err := ipfslog.NewFromEntry(ctx, ipfs, identities[2], []iface.IPFSLogEntry{items[1][len(items[1])-1]}, &ipfslog.LogOptions{}, &entry.FetchOptions{})
				c.So(err, ShouldBeNil)
				// Here we're creating a log from entries signed by peer A, B and C
				// "logA" accepts entries from peer C so we can join logs A and B
				logB, err := ipfslog.NewFromEntry(ctx, ipfs, identities[2], []iface.IPFSLogEntry{items[2][len(items[2])-1]}, &ipfslog.LogOptions{}, &entry.FetchOptions{})
				c.So(err, ShouldBeNil)

				_, err = logA.Join(logB, -1)
				c.So(err, ShouldBeNil)

				// The last entry, 'entryC100', should be the only head
				// (it points to entryB100, entryB100 and entryC99)
				c.So(len(entry.FindHeads(logA.Entries)), ShouldEqual, 1)
			})

			c.Convey("returns error if log parameter is not defined", FailureHalts, func() {
				_, err := logs[0].Join(nil, -1)
				c.So(err, ShouldEqual, errmsg.LogJoinNotDefined)
			})

			c.Convey("joins only unique items", FailureHalts, func() {
				_, err := logs[0].Append(ctx, []byte("helloA1"), 1)
				c.So(err, ShouldBeNil)
				_, err = logs[0].Append(ctx, []byte("helloA2"), 1)
				c.So(err, ShouldBeNil)
				_, err = logs[1].Append(ctx, []byte("helloB1"), 1)
				c.So(err, ShouldBeNil)
				_, err = logs[1].Append(ctx, []byte("helloB2"), 1)
				c.So(err, ShouldBeNil)

				_, err = logs[0].Join(logs[1], -1)
				c.So(err, ShouldBeNil)
				_, err = logs[0].Join(logs[1], -1)
				c.So(err, ShouldBeNil)

				c.So(logs[0].Values().Len(), ShouldEqual, 4)

				expected := []string{"helloA1", "helloB1", "helloA2", "helloB2"}
				var result []string

				for _, v := range logs[0].Values().Keys() {
					result = append(result, string(logs[0].Values().UnsafeGet(v).GetPayload()))
				}

				c.So(expected, ShouldResemble, result)
				c.So(len(getLastEntry(logs[0].Values()).GetNext()), ShouldEqual, 1)
			})

			c.Convey("joins logs two ways", FailureHalts, func() {
				_, err := logs[0].Append(ctx, []byte("helloA1"), 1)
				c.So(err, ShouldBeNil)
				_, err = logs[0].Append(ctx, []byte("helloA2"), 1)
				c.So(err, ShouldBeNil)
				_, err = logs[1].Append(ctx, []byte("helloB1"), 1)
				c.So(err, ShouldBeNil)
				_, err = logs[1].Append(ctx, []byte("helloB2"), 1)
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
						hashes[i] = append(hashes[i], v.GetHash())
						payloads[i] = append(payloads[i], v.GetPayload())
					}
				}

				c.So(reflect.DeepEqual(hashes[0], hashes[1]), ShouldBeTrue)
				// TODO: Add fixed key and enable the following tests
				_ = expected
				//c.So(payloads[0], ShouldResemble, expected)
				//c.So(payloads[1], ShouldResemble, expected)
			})

			c.Convey("joins logs twice", FailureHalts, func() {
				_, err := logs[0].Append(ctx, []byte("helloA1"), 1)
				c.So(err, ShouldBeNil)
				_, err = logs[1].Append(ctx, []byte("helloB1"), 1)
				c.So(err, ShouldBeNil)
				_, err = logs[1].Join(logs[0], -1)
				c.So(err, ShouldBeNil)

				_, err = logs[0].Append(ctx, []byte("helloA2"), 1)
				c.So(err, ShouldBeNil)
				_, err = logs[1].Append(ctx, []byte("helloB2"), 1)
				c.So(err, ShouldBeNil)
				_, err = logs[1].Join(logs[0], -1)
				c.So(err, ShouldBeNil)

				c.So(logs[1].Values().Len(), ShouldEqual, 4)

				expected := []string{"helloA1", "helloB1", "helloA2", "helloB2"}
				var result []string

				for _, v := range logs[1].Values().Keys() {
					result = append(result, string(logs[1].Values().UnsafeGet(v).GetPayload()))
				}

				c.So(expected, ShouldResemble, result)
			})

			c.Convey("joins 2 logs two ways", FailureHalts, func() {
				_, err := logs[0].Append(ctx, []byte("helloA1"), 1)
				c.So(err, ShouldBeNil)
				_, err = logs[1].Append(ctx, []byte("helloB1"), 1)
				c.So(err, ShouldBeNil)
				_, err = logs[1].Join(logs[0], -1)
				c.So(err, ShouldBeNil)
				_, err = logs[0].Join(logs[1], -1)
				c.So(err, ShouldBeNil)

				_, err = logs[0].Append(ctx, []byte("helloA2"), 1)
				c.So(err, ShouldBeNil)
				_, err = logs[1].Append(ctx, []byte("helloB2"), 1)
				c.So(err, ShouldBeNil)
				_, err = logs[1].Join(logs[0], -1)
				c.So(err, ShouldBeNil)

				c.So(logs[1].Values().Len(), ShouldEqual, 4)

				expected := []string{"helloA1", "helloB1", "helloA2", "helloB2"}
				var result []string

				for _, v := range logs[1].Values().Keys() {
					result = append(result, string(logs[1].Values().UnsafeGet(v).GetPayload()))
				}

				c.So(expected, ShouldResemble, result)
			})

			// c.Convey("joins 2 logs two ways and has the right heads at every step", FailureHalts, func() {
			// 	_, err := logs[0].Append([]byte("helloA1"), 1)
			// 	c.So(err, ShouldBeNil)
			// 	c.So(len(log.FindHeads(logs[0].Entries)), ShouldEqual, 1)
			// 	c.So(string(log.FindHeads(logs[0].Entries)[0].Payload), ShouldEqual, "helloA1")

			// 	_, err = logs[1].Append([]byte("helloB1"), 1)
			// 	c.So(err, ShouldBeNil)
			// 	c.So(len(log.FindHeads(logs[1].Entries)), ShouldEqual, 1)
			// 	c.So(string(log.FindHeads(logs[1].Entries)[0].Payload), ShouldEqual, "helloB1")

			// 	_, err = logs[1].Join(logs[0], -1)
			// 	c.So(err, ShouldBeNil)
			// 	c.So(len(log.FindHeads(logs[1].Entries)), ShouldEqual, 2)
			// 	c.So(string(log.FindHeads(logs[1].Entries)[0].Payload), ShouldEqual, "helloB1")
			// 	c.So(string(log.FindHeads(logs[1].Entries)[1].Payload), ShouldEqual, "helloA1")

			// 	_, err = logs[0].Join(logs[1], -1)
			// 	c.So(err, ShouldBeNil)
			// 	c.So(len(log.FindHeads(logs[0].Entries)), ShouldEqual, 2)
			// 	c.So(string(log.FindHeads(logs[0].Entries)[0].Payload), ShouldEqual, "helloB1")
			// 	c.So(string(log.FindHeads(logs[0].Entries)[1].Payload), ShouldEqual, "helloA1")

			// 	_, err = logs[0].Append([]byte("helloA2"), 1)
			// 	c.So(err, ShouldBeNil)
			// 	c.So(len(log.FindHeads(logs[0].Entries)), ShouldEqual, 1)
			// 	c.So(string(log.FindHeads(logs[0].Entries)[0].Payload), ShouldEqual, "helloA2")

			// 	_, err = logs[1].Append([]byte("helloB2"), 1)
			// 	c.So(err, ShouldBeNil)
			// 	c.So(len(log.FindHeads(logs[1].Entries)), ShouldEqual, 1)
			// 	c.So(string(log.FindHeads(logs[1].Entries)[0].Payload), ShouldEqual, "helloB1")

			// 	_, err = logs[1].Join(logs[0], -1)
			// 	c.So(err, ShouldBeNil)
			// 	c.So(len(log.FindHeads(logs[1].Entries)), ShouldEqual, 2)
			// 	c.So(string(log.FindHeads(logs[1].Entries)[0].Payload), ShouldEqual, "helloB2")
			// 	c.So(string(log.FindHeads(logs[1].Entries)[1].Payload), ShouldEqual, "helloA2")
			// })

			c.Convey("joins 4 logs to one", FailureHalts, func() {
				// order determined by identity's publicKey
				_, err := logs[0].Append(ctx, []byte("helloA1"), 1)
				c.So(err, ShouldBeNil)
				_, err = logs[0].Append(ctx, []byte("helloA2"), 1)
				c.So(err, ShouldBeNil)

				_, err = logs[2].Append(ctx, []byte("helloB1"), 1)
				c.So(err, ShouldBeNil)
				_, err = logs[2].Append(ctx, []byte("helloB2"), 1)
				c.So(err, ShouldBeNil)

				_, err = logs[1].Append(ctx, []byte("helloC1"), 1)
				c.So(err, ShouldBeNil)
				_, err = logs[1].Append(ctx, []byte("helloC2"), 1)
				c.So(err, ShouldBeNil)

				_, err = logs[3].Append(ctx, []byte("helloD1"), 1)
				c.So(err, ShouldBeNil)
				_, err = logs[3].Append(ctx, []byte("helloD2"), 1)
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
					result = append(result, string(logs[0].Values().UnsafeGet(v).GetPayload()))
				}

				c.So(expected, ShouldResemble, result)
			})

			c.Convey("joins 4 logs to one is commutative", FailureHalts, func() {
				_, err := logs[0].Append(ctx, []byte("helloA1"), 1)
				c.So(err, ShouldBeNil)
				_, err = logs[0].Append(ctx, []byte("helloA2"), 1)
				c.So(err, ShouldBeNil)

				_, err = logs[1].Append(ctx, []byte("helloB1"), 1)
				c.So(err, ShouldBeNil)
				_, err = logs[1].Append(ctx, []byte("helloB2"), 1)
				c.So(err, ShouldBeNil)

				_, err = logs[2].Append(ctx, []byte("helloC1"), 1)
				c.So(err, ShouldBeNil)
				_, err = logs[2].Append(ctx, []byte("helloC2"), 1)
				c.So(err, ShouldBeNil)

				_, err = logs[3].Append(ctx, []byte("helloD1"), 1)
				c.So(err, ShouldBeNil)
				_, err = logs[3].Append(ctx, []byte("helloD2"), 1)
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
						payloads[i] = append(payloads[i], string(logs[i].Values().UnsafeGet(v).GetPayload()))
					}
				}

				c.So(payloads[0], ShouldResemble, payloads[1])
			})

			c.Convey("joins logs and updates clocks", FailureHalts, func() {
				_, err := logs[0].Append(ctx, []byte("helloA1"), 1)
				c.So(err, ShouldBeNil)
				_, err = logs[1].Append(ctx, []byte("helloB1"), 1)
				c.So(err, ShouldBeNil)
				_, err = logs[1].Join(logs[0], -1)
				c.So(err, ShouldBeNil)
				_, err = logs[0].Append(ctx, []byte("helloA2"), 1)
				c.So(err, ShouldBeNil)
				_, err = logs[1].Append(ctx, []byte("helloB2"), 1)
				c.So(err, ShouldBeNil)

				c.So(logs[0].Clock.GetID(), ShouldResemble, identities[0].PublicKey)
				c.So(logs[1].Clock.GetID(), ShouldResemble, identities[1].PublicKey)
				c.So(logs[0].Clock.GetTime(), ShouldEqual, 2)
				c.So(logs[1].Clock.GetTime(), ShouldEqual, 2)

				_, err = logs[2].Join(logs[0], -1)
				c.So(err, ShouldBeNil)
				c.So(logs[2].ID, ShouldEqual, "X")
				c.So(logs[2].Clock.GetID(), ShouldResemble, identities[2].PublicKey)
				c.So(logs[2].Clock.GetTime(), ShouldEqual, 2)

				_, err = logs[2].Append(ctx, []byte("helloC1"), 1)
				c.So(err, ShouldBeNil)
				_, err = logs[2].Append(ctx, []byte("helloC2"), 1)
				c.So(err, ShouldBeNil)
				_, err = logs[0].Join(logs[2], -1)
				c.So(err, ShouldBeNil)
				_, err = logs[0].Join(logs[1], -1)
				c.So(err, ShouldBeNil)
				_, err = logs[3].Append(ctx, []byte("helloD1"), 1)
				c.So(err, ShouldBeNil)
				_, err = logs[3].Append(ctx, []byte("helloD2"), 1)
				c.So(err, ShouldBeNil)
				_, err = logs[3].Join(logs[1], -1)
				c.So(err, ShouldBeNil)
				_, err = logs[3].Join(logs[0], -1)
				c.So(err, ShouldBeNil)
				_, err = logs[3].Join(logs[2], -1)
				c.So(err, ShouldBeNil)
				_, err = logs[3].Append(ctx, []byte("helloD3"), 1)
				c.So(err, ShouldBeNil)
				_, err = logs[3].Append(ctx, []byte("helloD4"), 1)
				c.So(err, ShouldBeNil)

				_, err = logs[0].Join(logs[3], -1)
				c.So(err, ShouldBeNil)
				_, err = logs[3].Join(logs[0], -1)
				c.So(err, ShouldBeNil)
				_, err = logs[3].Append(ctx, []byte("helloD5"), 1)
				c.So(err, ShouldBeNil)
				_, err = logs[0].Append(ctx, []byte("helloA5"), 1)
				c.So(err, ShouldBeNil)
				_, err = logs[3].Join(logs[0], -1)
				c.So(err, ShouldBeNil)
				c.So(logs[3].Clock.GetID(), ShouldResemble, identities[3].PublicKey)
				c.So(logs[3].Clock.GetTime(), ShouldEqual, 7)

				_, err = logs[3].Append(ctx, []byte("helloD6"), 1)
				c.So(err, ShouldBeNil)
				c.So(logs[3].Clock.GetTime(), ShouldEqual, 8)

				expected := []entry.Entry{
					{Payload: []byte("helloA1"), LogID: "X", Clock: &entry.LamportClock{ID: identities[0].PublicKey, Time: 1}},
					{Payload: []byte("helloB1"), LogID: "X", Clock: &entry.LamportClock{ID: identities[1].PublicKey, Time: 1}},
					{Payload: []byte("helloD1"), LogID: "X", Clock: &entry.LamportClock{ID: identities[3].PublicKey, Time: 1}},
					{Payload: []byte("helloA2"), LogID: "X", Clock: &entry.LamportClock{ID: identities[0].PublicKey, Time: 2}},
					{Payload: []byte("helloB2"), LogID: "X", Clock: &entry.LamportClock{ID: identities[1].PublicKey, Time: 2}},
					{Payload: []byte("helloD2"), LogID: "X", Clock: &entry.LamportClock{ID: identities[3].PublicKey, Time: 2}},
					{Payload: []byte("helloC1"), LogID: "X", Clock: &entry.LamportClock{ID: identities[2].PublicKey, Time: 3}},
					{Payload: []byte("helloC2"), LogID: "X", Clock: &entry.LamportClock{ID: identities[2].PublicKey, Time: 4}},
					{Payload: []byte("helloD3"), LogID: "X", Clock: &entry.LamportClock{ID: identities[3].PublicKey, Time: 5}},
					{Payload: []byte("helloD4"), LogID: "X", Clock: &entry.LamportClock{ID: identities[3].PublicKey, Time: 6}},
					{Payload: []byte("helloA5"), LogID: "X", Clock: &entry.LamportClock{ID: identities[0].PublicKey, Time: 7}},
					{Payload: []byte("helloD5"), LogID: "X", Clock: &entry.LamportClock{ID: identities[3].PublicKey, Time: 7}},
					{Payload: []byte("helloD6"), LogID: "X", Clock: &entry.LamportClock{ID: identities[3].PublicKey, Time: 8}},
				}

				c.So(logs[3].Values().Len(), ShouldEqual, 13)

				var result []entry.Entry

				for _, v := range logs[3].Values().Keys() {
					e, exist := logs[3].Values().Get(v)
					c.So(exist, ShouldBeTrue)
					result = append(result, entry.Entry{Payload: e.GetPayload(), LogID: e.GetLogID(), Clock: &entry.LamportClock{
						ID:   e.GetClock().GetID(),
						Time: e.GetClock().GetTime(),
					}})
				}

				c.So(reflect.DeepEqual(result, expected), ShouldBeTrue)
			})

			c.Convey("joins logs from 4 logs", FailureHalts, func() {
				_, err := logs[0].Append(ctx, []byte("helloA1"), 1)
				c.So(err, ShouldBeNil)
				_, err = logs[0].Join(logs[1], -1)
				c.So(err, ShouldBeNil)
				_, err = logs[1].Append(ctx, []byte("helloB1"), 1)
				c.So(err, ShouldBeNil)
				_, err = logs[1].Join(logs[0], -1)
				c.So(err, ShouldBeNil)
				_, err = logs[0].Append(ctx, []byte("helloA2"), 1)
				c.So(err, ShouldBeNil)
				_, err = logs[1].Append(ctx, []byte("helloB2"), 1)
				c.So(err, ShouldBeNil)

				_, err = logs[0].Join(logs[2], -1)
				c.So(err, ShouldBeNil)
				c.So(logs[0].ID, ShouldEqual, "X")
				c.So(logs[0].Clock.GetID(), ShouldResemble, identities[0].PublicKey)
				c.So(logs[0].Clock.GetTime(), ShouldEqual, 2)

				_, err = logs[2].Join(logs[0], -1)
				c.So(err, ShouldBeNil)
				c.So(logs[2].ID, ShouldEqual, "X")
				c.So(logs[2].Clock.GetID(), ShouldResemble, identities[2].PublicKey)
				c.So(logs[2].Clock.GetTime(), ShouldEqual, 2)

				_, err = logs[2].Append(ctx, []byte("helloC1"), 1)
				c.So(err, ShouldBeNil)
				_, err = logs[2].Append(ctx, []byte("helloC2"), 1)
				c.So(err, ShouldBeNil)
				_, err = logs[0].Join(logs[2], -1)
				c.So(err, ShouldBeNil)
				_, err = logs[0].Join(logs[1], -1)
				c.So(err, ShouldBeNil)
				_, err = logs[3].Append(ctx, []byte("helloD1"), 1)
				c.So(err, ShouldBeNil)
				_, err = logs[3].Append(ctx, []byte("helloD2"), 1)
				c.So(err, ShouldBeNil)
				_, err = logs[3].Join(logs[1], -1)
				c.So(err, ShouldBeNil)
				_, err = logs[3].Join(logs[0], -1)
				c.So(err, ShouldBeNil)
				_, err = logs[3].Join(logs[2], -1)
				c.So(err, ShouldBeNil)
				_, err = logs[3].Append(ctx, []byte("helloD3"), 1)
				c.So(err, ShouldBeNil)
				_, err = logs[3].Append(ctx, []byte("helloD4"), 1)
				c.So(err, ShouldBeNil)

				c.So(logs[3].Clock.GetID(), ShouldResemble, identities[3].PublicKey)
				c.So(logs[3].Clock.GetTime(), ShouldEqual, 6)

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
					result = append(result, logs[3].Values().UnsafeGet(v).GetPayload())
				}

				c.So(reflect.DeepEqual(expected, result), ShouldBeTrue)
			})

			c.Convey("joins only specified amount of entries - one entry", FailureHalts, func() {
				_, err := logs[0].Append(ctx, []byte("helloA1"), 1)
				c.So(err, ShouldBeNil)
				_, err = logs[0].Append(ctx, []byte("helloA2"), 1)
				c.So(err, ShouldBeNil)
				_, err = logs[1].Append(ctx, []byte("helloB1"), 1)
				c.So(err, ShouldBeNil)
				_, err = logs[1].Append(ctx, []byte("helloB2"), 1)
				c.So(err, ShouldBeNil)

				_, err = logs[0].Join(logs[1], 1)
				c.So(err, ShouldBeNil)

				expected := [][]byte{[]byte("helloB2")}

				c.So(logs[0].Values().Len(), ShouldEqual, 1)

				var result [][]byte
				var key string

				for _, v := range logs[0].Values().Keys() {
					result = append(result, logs[0].Values().UnsafeGet(v).GetPayload())
					key = v
				}

				c.So(reflect.DeepEqual(expected, result), ShouldBeTrue)
				c.So(len(logs[0].Values().UnsafeGet(key).GetNext()), ShouldEqual, 1)
			})

			c.Convey("joins only specified amount of entries - two entries", FailureHalts, func() {
				_, err := logs[0].Append(ctx, []byte("helloA1"), 1)
				c.So(err, ShouldBeNil)
				_, err = logs[0].Append(ctx, []byte("helloA2"), 1)
				c.So(err, ShouldBeNil)
				_, err = logs[1].Append(ctx, []byte("helloB1"), 1)
				c.So(err, ShouldBeNil)
				_, err = logs[1].Append(ctx, []byte("helloB2"), 1)
				c.So(err, ShouldBeNil)

				_, err = logs[0].Join(logs[1], 2)
				c.So(err, ShouldBeNil)

				expected := [][]byte{[]byte("helloA2"), []byte("helloB2")}

				c.So(logs[0].Values().Len(), ShouldEqual, 2)

				var result [][]byte
				var key string

				for _, v := range logs[0].Values().Keys() {
					result = append(result, logs[0].Values().UnsafeGet(v).GetPayload())
					key = v
				}

				c.So(reflect.DeepEqual(expected, result), ShouldBeTrue)
				c.So(len(logs[0].Values().UnsafeGet(key).GetNext()), ShouldEqual, 1)
			})

			c.Convey("joins only specified amount of entries - three entries", FailureHalts, func() {
				_, err := logs[0].Append(ctx, []byte("helloA1"), 1)
				c.So(err, ShouldBeNil)
				_, err = logs[0].Append(ctx, []byte("helloA2"), 1)
				c.So(err, ShouldBeNil)
				_, err = logs[1].Append(ctx, []byte("helloB1"), 1)
				c.So(err, ShouldBeNil)
				_, err = logs[1].Append(ctx, []byte("helloB2"), 1)
				c.So(err, ShouldBeNil)

				_, err = logs[0].Join(logs[1], 3)
				c.So(err, ShouldBeNil)

				expected := [][]byte{[]byte("helloB1"), []byte("helloA2"), []byte("helloB2")}

				c.So(logs[0].Values().Len(), ShouldEqual, 3)

				var result [][]byte
				var key string

				for _, v := range logs[0].Values().Keys() {
					result = append(result, logs[0].Values().UnsafeGet(v).GetPayload())
					key = v
				}

				c.So(reflect.DeepEqual(expected, result), ShouldBeTrue)
				c.So(len(logs[0].Values().UnsafeGet(key).GetNext()), ShouldEqual, 1)
			})

			c.Convey("joins only specified amount of entries - (all) four entries", FailureHalts, func() {
				_, err := logs[0].Append(ctx, []byte("helloA1"), 1)
				c.So(err, ShouldBeNil)
				_, err = logs[0].Append(ctx, []byte("helloA2"), 1)
				c.So(err, ShouldBeNil)
				_, err = logs[1].Append(ctx, []byte("helloB1"), 1)
				c.So(err, ShouldBeNil)
				_, err = logs[1].Append(ctx, []byte("helloB2"), 1)
				c.So(err, ShouldBeNil)

				_, err = logs[0].Join(logs[1], 4)
				c.So(err, ShouldBeNil)

				expected := [][]byte{[]byte("helloA1"), []byte("helloB1"), []byte("helloA2"), []byte("helloB2")}

				c.So(logs[0].Values().Len(), ShouldEqual, 4)

				var result [][]byte
				var key string

				for _, v := range logs[0].Values().Keys() {
					result = append(result, logs[0].Values().UnsafeGet(v).GetPayload())
					key = v
				}

				c.So(reflect.DeepEqual(expected, result), ShouldBeTrue)
				c.So(len(logs[0].Values().UnsafeGet(key).GetNext()), ShouldEqual, 1)
			})
		})
	})
}
