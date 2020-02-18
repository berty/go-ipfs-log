package test // import "berty.tech/go-ipfs-log/test"

import (
	"context"
	"fmt"
	"testing"

	mocknet "github.com/libp2p/go-libp2p/p2p/net/mock"

	ipfslog "berty.tech/go-ipfs-log"

	"berty.tech/go-ipfs-log/entry"

	ds "github.com/ipfs/go-datastore"
	dssync "github.com/ipfs/go-datastore/sync"

	idp "berty.tech/go-ipfs-log/identityprovider"
	ks "berty.tech/go-ipfs-log/keystore"

	. "github.com/smartystreets/goconvey/convey"
)

func TestLogHeadsTails(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	m := mocknet.New(ctx)
	ipfs, closeNode := NewMemoryServices(ctx, t, m)
	defer closeNode()

	datastore := dssync.MutexWrap(ds.NewMapDatastore())
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

	Convey("IPFSLog - heads and Tails", t, FailureContinues, func(c C) {
		c.Convey("heads", FailureContinues, func(c C) {
			c.Convey("finds one head after one entry", FailureContinues, func(c C) {
				log1, err := ipfslog.NewLog(ipfs, identities[0], &ipfslog.LogOptions{ID: "A"})
				c.So(err, ShouldBeNil)
				_, err = log1.Append(ctx, []byte("helloA1"), nil)
				c.So(err, ShouldBeNil)

				c.So(len(entry.FindHeads(log1.Entries)), ShouldEqual, 1)
			})

			c.Convey("finds one head after two entry", FailureContinues, func(c C) {
				log1, err := ipfslog.NewLog(ipfs, identities[0], &ipfslog.LogOptions{ID: "A"})
				c.So(err, ShouldBeNil)
				_, err = log1.Append(ctx, []byte("helloA1"), nil)
				c.So(err, ShouldBeNil)
				_, err = log1.Append(ctx, []byte("helloA2"), nil)
				c.So(err, ShouldBeNil)

				c.So(len(entry.FindHeads(log1.Entries)), ShouldEqual, 1)
			})

			c.Convey("finds head after a join and append", FailureContinues, func(c C) {
				log1, err := ipfslog.NewLog(ipfs, identities[0], &ipfslog.LogOptions{ID: "A"})
				c.So(err, ShouldBeNil)
				log2, err := ipfslog.NewLog(ipfs, identities[0], &ipfslog.LogOptions{ID: "A"})
				c.So(err, ShouldBeNil)

				_, err = log1.Append(ctx, []byte("helloA1"), nil)
				c.So(err, ShouldBeNil)
				_, err = log1.Append(ctx, []byte("helloA2"), nil)
				c.So(err, ShouldBeNil)
				_, err = log2.Append(ctx, []byte("helloB1"), nil)
				c.So(err, ShouldBeNil)

				_, err = log2.Join(log1, -1)
				c.So(err, ShouldBeNil)
				_, err = log2.Append(ctx, []byte("helloB2"), nil)
				c.So(err, ShouldBeNil)

				lastEntry := getLastEntry(log2.Values())

				c.So(len(entry.FindHeads(log2.Entries)), ShouldEqual, 1)
				c.So(entry.FindHeads(log2.Entries)[0].GetHash().String(), ShouldEqual, lastEntry.GetHash().String())
			})

			c.Convey("finds two heads after a join", FailureContinues, func(c C) {
				log1, err := ipfslog.NewLog(ipfs, identities[0], &ipfslog.LogOptions{ID: "A"})
				c.So(err, ShouldBeNil)
				log2, err := ipfslog.NewLog(ipfs, identities[0], &ipfslog.LogOptions{ID: "A"})
				c.So(err, ShouldBeNil)

				_, err = log1.Append(ctx, []byte("helloA1"), nil)
				c.So(err, ShouldBeNil)
				_, err = log1.Append(ctx, []byte("helloA2"), nil)
				c.So(err, ShouldBeNil)
				lastEntry1 := getLastEntry(log1.Values())

				_, err = log2.Append(ctx, []byte("helloB1"), nil)
				c.So(err, ShouldBeNil)
				_, err = log2.Append(ctx, []byte("helloB2"), nil)
				c.So(err, ShouldBeNil)
				lastEntry2 := getLastEntry(log2.Values())

				_, err = log1.Join(log2, -1)
				c.So(err, ShouldBeNil)

				c.So(len(entry.FindHeads(log1.Entries)), ShouldEqual, 2)
				c.So(entry.FindHeads(log1.Entries)[0].GetHash().String(), ShouldEqual, lastEntry1.GetHash().String())
				c.So(entry.FindHeads(log1.Entries)[1].GetHash().String(), ShouldEqual, lastEntry2.GetHash().String())
			})

			c.Convey("finds two heads after two joins", FailureContinues, func(c C) {
				log1, err := ipfslog.NewLog(ipfs, identities[0], &ipfslog.LogOptions{ID: "A"})
				c.So(err, ShouldBeNil)
				log2, err := ipfslog.NewLog(ipfs, identities[0], &ipfslog.LogOptions{ID: "A"})
				c.So(err, ShouldBeNil)

				_, err = log1.Append(ctx, []byte("helloA1"), nil)
				c.So(err, ShouldBeNil)
				_, err = log1.Append(ctx, []byte("helloA2"), nil)
				c.So(err, ShouldBeNil)

				_, err = log2.Append(ctx, []byte("helloB1"), nil)
				c.So(err, ShouldBeNil)
				_, err = log2.Append(ctx, []byte("helloB2"), nil)
				c.So(err, ShouldBeNil)

				_, err = log1.Join(log2, -1)
				c.So(err, ShouldBeNil)

				_, err = log2.Append(ctx, []byte("helloB3"), nil)
				c.So(err, ShouldBeNil)

				_, err = log1.Append(ctx, []byte("helloA3"), nil)
				c.So(err, ShouldBeNil)
				_, err = log1.Append(ctx, []byte("helloA4"), nil)
				c.So(err, ShouldBeNil)

				lastEntry1 := getLastEntry(log1.Values())
				lastEntry2 := getLastEntry(log2.Values())

				_, err = log1.Join(log2, -1)
				c.So(err, ShouldBeNil)

				c.So(len(entry.FindHeads(log1.Entries)), ShouldEqual, 2)
				c.So(entry.FindHeads(log1.Entries)[0].GetHash().String(), ShouldEqual, lastEntry1.GetHash().String())
				c.So(entry.FindHeads(log1.Entries)[1].GetHash().String(), ShouldEqual, lastEntry2.GetHash().String())
			})

			c.Convey("finds two heads after three joins", FailureContinues, func(c C) {
				log1, err := ipfslog.NewLog(ipfs, identities[0], &ipfslog.LogOptions{ID: "A"})
				c.So(err, ShouldBeNil)
				log2, err := ipfslog.NewLog(ipfs, identities[0], &ipfslog.LogOptions{ID: "A"})
				c.So(err, ShouldBeNil)
				log3, err := ipfslog.NewLog(ipfs, identities[0], &ipfslog.LogOptions{ID: "A"})
				c.So(err, ShouldBeNil)

				_, err = log1.Append(ctx, []byte("helloA1"), nil)
				c.So(err, ShouldBeNil)
				_, err = log1.Append(ctx, []byte("helloA2"), nil)
				c.So(err, ShouldBeNil)
				_, err = log2.Append(ctx, []byte("helloB1"), nil)
				c.So(err, ShouldBeNil)
				_, err = log2.Append(ctx, []byte("helloB2"), nil)
				c.So(err, ShouldBeNil)
				_, err = log1.Join(log2, -1)
				c.So(err, ShouldBeNil)
				_, err = log1.Append(ctx, []byte("helloA3"), nil)
				c.So(err, ShouldBeNil)
				_, err = log1.Append(ctx, []byte("helloA4"), nil)
				c.So(err, ShouldBeNil)
				lastEntry1 := getLastEntry(log1.Values())
				_, err = log3.Append(ctx, []byte("helloC1"), nil)
				c.So(err, ShouldBeNil)
				_, err = log3.Append(ctx, []byte("helloC2"), nil)
				c.So(err, ShouldBeNil)
				_, err = log2.Join(log3, -1)
				c.So(err, ShouldBeNil)
				_, err = log2.Append(ctx, []byte("helloB3"), nil)
				c.So(err, ShouldBeNil)
				lastEntry2 := getLastEntry(log2.Values())
				_, err = log1.Join(log2, -1)
				c.So(err, ShouldBeNil)

				c.So(len(entry.FindHeads(log1.Entries)), ShouldEqual, 2)
				c.So(entry.FindHeads(log1.Entries)[0].GetHash().String(), ShouldEqual, lastEntry1.GetHash().String())
				c.So(entry.FindHeads(log1.Entries)[1].GetHash().String(), ShouldEqual, lastEntry2.GetHash().String())
			})

			c.Convey("finds three heads after three joins", FailureContinues, func(c C) {
				log1, err := ipfslog.NewLog(ipfs, identities[0], &ipfslog.LogOptions{ID: "A"})
				c.So(err, ShouldBeNil)
				log2, err := ipfslog.NewLog(ipfs, identities[0], &ipfslog.LogOptions{ID: "A"})
				c.So(err, ShouldBeNil)
				log3, err := ipfslog.NewLog(ipfs, identities[0], &ipfslog.LogOptions{ID: "A"})
				c.So(err, ShouldBeNil)

				_, err = log1.Append(ctx, []byte("helloA1"), nil)
				c.So(err, ShouldBeNil)
				_, err = log1.Append(ctx, []byte("helloA2"), nil)
				c.So(err, ShouldBeNil)
				_, err = log2.Append(ctx, []byte("helloB1"), nil)
				c.So(err, ShouldBeNil)
				_, err = log2.Append(ctx, []byte("helloB2"), nil)
				c.So(err, ShouldBeNil)
				_, err = log1.Join(log2, -1)
				c.So(err, ShouldBeNil)
				_, err = log1.Append(ctx, []byte("helloA3"), nil)
				c.So(err, ShouldBeNil)
				_, err = log1.Append(ctx, []byte("helloA4"), nil)
				c.So(err, ShouldBeNil)
				lastEntry1 := getLastEntry(log1.Values())
				_, err = log3.Append(ctx, []byte("helloC1"), nil)
				c.So(err, ShouldBeNil)
				_, err = log2.Append(ctx, []byte("helloB3"), nil)
				c.So(err, ShouldBeNil)
				_, err = log3.Append(ctx, []byte("helloC2"), nil)
				c.So(err, ShouldBeNil)
				lastEntry2 := getLastEntry(log2.Values())
				lastEntry3 := getLastEntry(log3.Values())
				_, err = log1.Join(log2, -1)
				c.So(err, ShouldBeNil)
				_, err = log1.Join(log3, -1)
				c.So(err, ShouldBeNil)

				c.So(len(entry.FindHeads(log1.Entries)), ShouldEqual, 3)
				c.So(entry.FindHeads(log1.Entries)[0].GetHash().String(), ShouldEqual, lastEntry1.GetHash().String())
				c.So(entry.FindHeads(log1.Entries)[1].GetHash().String(), ShouldEqual, lastEntry2.GetHash().String())
				c.So(entry.FindHeads(log1.Entries)[2].GetHash().String(), ShouldEqual, lastEntry3.GetHash().String())
			})
		})

		c.Convey("tails", FailureContinues, func(c C) {
			// TODO: implements findTails(orderedmap)
			// c.Convey("returns a tail", FailureContinues, func(c C) {
			// 	log1, err := log.NewLog(ipfs, identities[0], &log.LogOptions{ID: "A"})
			// 	c.So(err, ShouldBeNil)
			// 	_, err = log1.Append([]byte("helloA1"), nil)
			// 	c.So(err, ShouldBeNil)

			// 	c.So(len(log.FindTails(log1.Entries)), ShouldEqual, 1)
			// })

			// c.Convey("returns tail entries", FailureContinues, func(c C) {
			// 	log1, err := log.NewLog(ipfs, identities[0], &log.LogOptions{ID: "A"})
			// 	c.So(err, ShouldBeNil)
			// 	log2, err := log.NewLog(ipfs, identities[0], &log.LogOptions{ID: "A"})
			// 	c.So(err, ShouldBeNil)
			// 	_, err = log1.Append([]byte("helloA1"), nil)
			// 	c.So(err, ShouldBeNil)
			// 	_, err = log1.Append([]byte("helloA1"), nil)
			// 	c.So(err, ShouldBeNil)

			// 	c.So(len(log.FindTails(log1.Entries)), ShouldEqual, 1)
			// })
		})
	})
}
