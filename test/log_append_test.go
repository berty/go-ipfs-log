package test // import "berty.tech/go-ipfs-log/test"

import (
	"context"
	"fmt"
	"math"
	"testing"
	"time"

	mocknet "github.com/libp2p/go-libp2p/p2p/net/mock"

	ipfslog "berty.tech/go-ipfs-log"
	"berty.tech/go-ipfs-log/entry"
	"berty.tech/go-ipfs-log/iface"

	idp "berty.tech/go-ipfs-log/identityprovider"
	"berty.tech/go-ipfs-log/keystore"
	dssync "github.com/ipfs/go-datastore/sync"

	. "github.com/smartystreets/goconvey/convey"
)

func TestLogAppend(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*10)
	defer cancel()

	m := mocknet.New(ctx)
	ipfs, closeNode := NewMemoryServices(ctx, t, m)
	defer closeNode()

	datastore := dssync.MutexWrap(NewIdentityDataStore(t))
	keystore, err := keystore.NewKeystore(datastore)
	if err != nil {
		t.Fatal(err)
	}

	identity, err := idp.CreateIdentity(&idp.CreateIdentityOptions{
		Keystore: keystore,
		ID:       fmt.Sprintf("userA"),
		Type:     "orbitdb",
	})

	if err != nil {
		t.Fatal(err)
	}

	Convey("IPFSLog - Append", t, FailureHalts, func(c C) {
		c.Convey("append", FailureHalts, func(c C) {
			c.Convey("append one", FailureHalts, func(c C) {
				log1, err := ipfslog.NewLog(ipfs, identity, &ipfslog.LogOptions{ID: "A"})
				c.So(err, ShouldBeNil)
				_, err = log1.Append(ctx, []byte("hello1"), nil)
				c.So(err, ShouldBeNil)

				c.So(log1.Entries.Len(), ShouldEqual, 1)
				values := log1.Values()
				keys := values.Keys()

				for _, k := range keys {
					v := values.UnsafeGet(k)
					c.So(string(v.GetPayload()), ShouldEqual, "hello1")
					c.So(len(v.GetNext()), ShouldEqual, 0)
					c.So(v.GetClock().GetID(), ShouldResemble, identity.PublicKey)
					c.So(v.GetClock().GetTime(), ShouldEqual, 1)
				}
				for _, v := range entry.FindHeads(log1.Entries) {
					c.So(v.GetHash().String(), ShouldEqual, values.UnsafeGet(keys[0]).GetHash().String())
				}
			})

			c.Convey("append 100 items to a log", FailureHalts, func(c C) {
				log1, err := ipfslog.NewLog(ipfs, identity, &ipfslog.LogOptions{ID: "A"})
				c.So(err, ShouldBeNil)
				nextPointerAmount := 64

				for i := 0; i < 100; i++ {
					_, err := log1.Append(ctx, []byte(fmt.Sprintf("hello%d", i)), &iface.AppendOptions{
						PointerCount: nextPointerAmount,
					})
					c.So(err, ShouldBeNil)

					values := log1.Values()
					keys := values.Keys()
					heads := entry.FindHeads(log1.Entries)

					c.So(len(heads), ShouldEqual, 1)
					c.So(heads[0].GetHash().String(), ShouldEqual, values.UnsafeGet(keys[len(keys)-1]).GetHash().String())
				}

				c.So(log1.Entries.Len(), ShouldEqual, 100)

				values := log1.Values()
				keys := values.Keys()

				for i, k := range keys {
					v := values.UnsafeGet(k)

					c.So(string(v.GetPayload()), ShouldEqual, fmt.Sprintf("hello%d", i))
					c.So(v.GetClock().GetTime(), ShouldEqual, i+1)
					c.So(v.GetClock().GetID(), ShouldResemble, identity.PublicKey)

					if i == 0 {
						c.So(len(v.GetRefs()), ShouldEqual, 0)
					} else {
						expected := math.Ceil(math.Log2(math.Min(float64(nextPointerAmount), float64(i))))

						c.So(len(v.GetRefs()), ShouldEqual, int(expected))
					}
				}
			})
		})
	})
}
