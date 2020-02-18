package test

import (
	"context"
	"fmt"
	"testing"

	dssync "github.com/ipfs/go-datastore/sync"
	mocknet "github.com/libp2p/go-libp2p/p2p/net/mock"
	. "github.com/smartystreets/goconvey/convey"

	ipfslog "berty.tech/go-ipfs-log"
	"berty.tech/go-ipfs-log/entry/sorting"
	idp "berty.tech/go-ipfs-log/identityprovider"
	ks "berty.tech/go-ipfs-log/keystore"
)

func TestLogJoinConcurrent(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	m := mocknet.New(ctx)
	ipfs, closeNode := NewMemoryServices(ctx, t, m)
	defer closeNode()

	datastore := dssync.MutexWrap(NewIdentityDataStore(t))
	keystore, err := ks.NewKeystore(datastore)
	if err != nil {
		t.Fatal(err)
	}
	Convey("join", t, FailureHalts, func(c C) {
		identity, err := idp.CreateIdentity(&idp.CreateIdentityOptions{
			Keystore: keystore,
			ID:       "userA",
			Type:     "orbitdb",
		})
		c.So(err, ShouldBeNil)

		log1, err := ipfslog.NewLog(ipfs, identity, &ipfslog.LogOptions{ID: "A", SortFn: sorting.SortByEntryHash})
		c.So(err, ShouldBeNil)

		log2, err := ipfslog.NewLog(ipfs, identity, &ipfslog.LogOptions{ID: "A", SortFn: sorting.SortByEntryHash})
		c.So(err, ShouldBeNil)

		// joins consistently
		for i := 0; i < 10; i++ {
			_, err = log1.Append(ctx, []byte(fmt.Sprintf("hello1-%d", i)), nil)
			c.So(err, ShouldBeNil)

			_, err = log2.Append(ctx, []byte(fmt.Sprintf("hello2-%d", i)), nil)
			c.So(err, ShouldBeNil)
		}

		_, err = log1.Join(log2, -1)
		c.So(err, ShouldBeNil)

		_, err = log2.Join(log1, -1)
		c.So(err, ShouldBeNil)

		hash1, err := log1.ToMultihash(ctx)
		c.So(err, ShouldBeNil)

		hash2, err := log2.ToMultihash(ctx)
		c.So(err, ShouldBeNil)

		c.So(hash1.Equals(hash2), ShouldBeTrue)
		c.So(log1.Values().Len(), ShouldEqual, 20)
		c.So(log1.ToString(nil), ShouldEqual, log2.ToString(nil))

		// Concurrently appending same payload after join results in same state
		for i := 10; i < 20; i++ {
			_, err = log1.Append(ctx, []byte(fmt.Sprintf("hello1-%d", i)), nil)
			c.So(err, ShouldBeNil)

			_, err = log2.Append(ctx, []byte(fmt.Sprintf("hello2-%d", i)), nil)
			c.So(err, ShouldBeNil)
		}

		_, err = log1.Join(log2, -1)
		c.So(err, ShouldBeNil)

		_, err = log2.Join(log1, -1)
		c.So(err, ShouldBeNil)

		_, err = log1.Append(ctx, []byte("same"), nil)
		c.So(err, ShouldBeNil)

		_, err = log2.Append(ctx, []byte("same"), nil)
		c.So(err, ShouldBeNil)

		hash1, err = log1.ToMultihash(ctx)
		c.So(err, ShouldBeNil)

		hash2, err = log2.ToMultihash(ctx)
		c.So(err, ShouldBeNil)

		c.So(hash1.Equals(hash2), ShouldBeTrue)
		c.So(log1.Values().Len(), ShouldEqual, 41)
		c.So(log2.Values().Len(), ShouldEqual, 41)
		c.So(log1.ToString(nil), ShouldEqual, log2.ToString(nil))

		// Joining after concurrently appending same payload joins entry once
		_, err = log1.Join(log2, -1)
		c.So(err, ShouldBeNil)

		_, err = log2.Join(log1, -1)
		c.So(err, ShouldBeNil)

		c.So(log1.Entries.Len(), ShouldEqual, log2.Entries.Len())
		c.So(log1.Entries.Len(), ShouldEqual, 41)
		c.So(log1.ToString(nil), ShouldEqual, log2.ToString(nil))
	})
}
