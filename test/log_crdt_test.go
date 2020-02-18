package test

import (
	"context"
	"fmt"
	"testing"

	dssync "github.com/ipfs/go-datastore/sync"
	mocknet "github.com/libp2p/go-libp2p/p2p/net/mock"
	. "github.com/smartystreets/goconvey/convey"

	ipfslog "berty.tech/go-ipfs-log"
	idp "berty.tech/go-ipfs-log/identityprovider"
	ks "berty.tech/go-ipfs-log/keystore"
)

func TestLogCRDT(t *testing.T) {
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

	var identities [3]*idp.Identity

	for i, char := range []rune{'A', 'B', 'C'} {
		identity, err := idp.CreateIdentity(&idp.CreateIdentityOptions{
			Keystore: keystore,
			ID:       fmt.Sprintf("user%c", char),
			Type:     "orbitdb",
		})

		if err != nil {
			t.Fatal(err)
		}

		identities[i] = identity
	}

	Convey("Log - CRDT", t, FailureHalts, func(c C) {
		log1, err := ipfslog.NewLog(ipfs, identities[0], &ipfslog.LogOptions{ID: "X"})
		c.So(err, ShouldBeNil)

		log2, err := ipfslog.NewLog(ipfs, identities[1], &ipfslog.LogOptions{ID: "X"})
		c.So(err, ShouldBeNil)

		log3, err := ipfslog.NewLog(ipfs, identities[2], &ipfslog.LogOptions{ID: "X"})
		c.So(err, ShouldBeNil)

		Convey("join is associative", FailureHalts, func(c C) {
			const expectedElementsCount = 6

			_, err = log1.Append(ctx, []byte("helloA1"), nil)
			c.So(err, ShouldBeNil)
			_, err = log1.Append(ctx, []byte("helloA2"), nil)
			c.So(err, ShouldBeNil)
			_, err = log2.Append(ctx, []byte("helloB1"), nil)
			c.So(err, ShouldBeNil)
			_, err = log2.Append(ctx, []byte("helloB2"), nil)
			c.So(err, ShouldBeNil)
			_, err = log3.Append(ctx, []byte("helloC1"), nil)
			c.So(err, ShouldBeNil)
			_, err = log3.Append(ctx, []byte("helloC2"), nil)
			c.So(err, ShouldBeNil)

			// a + (b + c)
			_, err = log2.Join(log3, -1)
			c.So(err, ShouldBeNil)

			_, err = log1.Join(log2, -1)
			c.So(err, ShouldBeNil)

			res1 := log1.ToString(nil)
			res1Len := log1.Values().Len()

			log1, err = ipfslog.NewLog(ipfs, identities[0], &ipfslog.LogOptions{ID: "X"})
			c.So(err, ShouldBeNil)

			log2, err = ipfslog.NewLog(ipfs, identities[1], &ipfslog.LogOptions{ID: "X"})
			c.So(err, ShouldBeNil)

			log3, err = ipfslog.NewLog(ipfs, identities[2], &ipfslog.LogOptions{ID: "X"})
			c.So(err, ShouldBeNil)

			_, err = log1.Append(ctx, []byte("helloA1"), nil)
			c.So(err, ShouldBeNil)

			_, err = log1.Append(ctx, []byte("helloA2"), nil)
			c.So(err, ShouldBeNil)

			_, err = log2.Append(ctx, []byte("helloB1"), nil)
			c.So(err, ShouldBeNil)

			_, err = log2.Append(ctx, []byte("helloB2"), nil)
			c.So(err, ShouldBeNil)

			_, err = log3.Append(ctx, []byte("helloC1"), nil)
			c.So(err, ShouldBeNil)

			_, err = log3.Append(ctx, []byte("helloC2"), nil)
			c.So(err, ShouldBeNil)

			// (a + b) + c
			_, err = log1.Join(log2, -1)
			c.So(err, ShouldBeNil)

			_, err = log3.Join(log1, -1)
			c.So(err, ShouldBeNil)

			res2 := log3.ToString(nil)
			res2Len := log3.Values().Len()

			// associativity: a + (b + c) == (a + b) + c

			c.So(res1Len, ShouldEqual, expectedElementsCount)
			c.So(res2Len, ShouldEqual, expectedElementsCount)
			c.So(res1, ShouldEqual, res2)
		})

		Convey("join is commutative", FailureHalts, func(c C) {
			const expectedElementsCount = 4

			_, err = log1.Append(ctx, []byte("helloA1"), nil)
			c.So(err, ShouldBeNil)

			_, err = log1.Append(ctx, []byte("helloA2"), nil)
			c.So(err, ShouldBeNil)

			_, err = log2.Append(ctx, []byte("helloB1"), nil)
			c.So(err, ShouldBeNil)

			_, err = log2.Append(ctx, []byte("helloB2"), nil)
			c.So(err, ShouldBeNil)

			// b + a
			_, err = log2.Join(log1, -1)
			c.So(err, ShouldBeNil)

			res1 := log2.ToString(nil)
			res1Len := log2.Values().Len()

			log1, err = ipfslog.NewLog(ipfs, identities[0], &ipfslog.LogOptions{ID: "X"})
			c.So(err, ShouldBeNil)

			log2, err = ipfslog.NewLog(ipfs, identities[1], &ipfslog.LogOptions{ID: "X"})
			c.So(err, ShouldBeNil)

			_, err = log1.Append(ctx, []byte("helloA1"), nil)
			c.So(err, ShouldBeNil)

			_, err = log1.Append(ctx, []byte("helloA2"), nil)
			c.So(err, ShouldBeNil)

			_, err = log2.Append(ctx, []byte("helloB1"), nil)
			c.So(err, ShouldBeNil)

			_, err = log2.Append(ctx, []byte("helloB2"), nil)
			c.So(err, ShouldBeNil)

			// a + b
			_, err = log1.Join(log2, -1)
			c.So(err, ShouldBeNil)

			res2 := log1.ToString(nil)
			res2Len := log1.Values().Len()

			// commutativity: a + b == b + a
			c.So(res1Len, ShouldEqual, expectedElementsCount)
			c.So(res2Len, ShouldEqual, expectedElementsCount)
			c.So(res1, ShouldEqual, res2)
		})

		Convey("multiple joins are commutative", FailureHalts, func(c C) {
			// b + a == a + b
			log1, err := ipfslog.NewLog(ipfs, identities[0], &ipfslog.LogOptions{ID: "X"})
			c.So(err, ShouldBeNil)

			log2, err := ipfslog.NewLog(ipfs, identities[1], &ipfslog.LogOptions{ID: "X"})
			c.So(err, ShouldBeNil)

			_, err = log1.Append(ctx, []byte("helloA1"), nil)
			c.So(err, ShouldBeNil)

			_, err = log1.Append(ctx, []byte("helloA2"), nil)
			c.So(err, ShouldBeNil)

			_, err = log2.Append(ctx, []byte("helloB1"), nil)
			c.So(err, ShouldBeNil)

			_, err = log2.Append(ctx, []byte("helloB2"), nil)
			c.So(err, ShouldBeNil)

			_, err = log2.Join(log1, -1)

			resA1 := log2.ToString(nil)

			log1, err = ipfslog.NewLog(ipfs, identities[0], &ipfslog.LogOptions{ID: "X"})
			c.So(err, ShouldBeNil)

			log2, err = ipfslog.NewLog(ipfs, identities[1], &ipfslog.LogOptions{ID: "X"})
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

			resA2 := log1.ToString(nil)

			c.So(resA1, ShouldEqual, resA2)

			// a + b == b + a
			log1, err = ipfslog.NewLog(ipfs, identities[0], &ipfslog.LogOptions{ID: "X"})
			c.So(err, ShouldBeNil)

			log2, err = ipfslog.NewLog(ipfs, identities[1], &ipfslog.LogOptions{ID: "X"})
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

			resB1 := log1.ToString(nil)

			log1, err = ipfslog.NewLog(ipfs, identities[0], &ipfslog.LogOptions{ID: "X"})
			c.So(err, ShouldBeNil)

			log2, err = ipfslog.NewLog(ipfs, identities[1], &ipfslog.LogOptions{ID: "X"})
			c.So(err, ShouldBeNil)

			_, err = log1.Append(ctx, []byte("helloA1"), nil)
			c.So(err, ShouldBeNil)

			_, err = log1.Append(ctx, []byte("helloA2"), nil)
			c.So(err, ShouldBeNil)

			_, err = log2.Append(ctx, []byte("helloB1"), nil)
			c.So(err, ShouldBeNil)

			_, err = log2.Append(ctx, []byte("helloB2"), nil)
			c.So(err, ShouldBeNil)

			_, err = log2.Join(log1, -1)
			c.So(err, ShouldBeNil)

			resB2 := log2.ToString(nil)

			c.So(resB1, ShouldEqual, resB2)

			// a + c == c + a
			log1, err = ipfslog.NewLog(ipfs, identities[0], &ipfslog.LogOptions{ID: "X"})
			c.So(err, ShouldBeNil)

			log3, err = ipfslog.NewLog(ipfs, identities[2], &ipfslog.LogOptions{ID: "X"})
			c.So(err, ShouldBeNil)

			_, err = log1.Append(ctx, []byte("helloA1"), nil)
			c.So(err, ShouldBeNil)

			_, err = log1.Append(ctx, []byte("helloA2"), nil)
			c.So(err, ShouldBeNil)

			_, err = log3.Append(ctx, []byte("helloC1"), nil)
			c.So(err, ShouldBeNil)

			_, err = log3.Append(ctx, []byte("helloC2"), nil)
			c.So(err, ShouldBeNil)

			_, err = log3.Join(log1, -1)
			c.So(err, ShouldBeNil)

			resC1 := log3.ToString(nil)

			log1, err = ipfslog.NewLog(ipfs, identities[0], &ipfslog.LogOptions{ID: "X"})
			c.So(err, ShouldBeNil)

			log3, err = ipfslog.NewLog(ipfs, identities[2], &ipfslog.LogOptions{ID: "X"})
			c.So(err, ShouldBeNil)

			_, err = log1.Append(ctx, []byte("helloA1"), nil)
			c.So(err, ShouldBeNil)

			_, err = log1.Append(ctx, []byte("helloA2"), nil)
			c.So(err, ShouldBeNil)

			_, err = log3.Append(ctx, []byte("helloC1"), nil)
			c.So(err, ShouldBeNil)

			_, err = log3.Append(ctx, []byte("helloC2"), nil)
			c.So(err, ShouldBeNil)

			_, err = log1.Join(log3, -1)

			resC2 := log1.ToString(nil)

			c.So(resC1, ShouldEqual, resC2)

			// c + b == b + c
			log2, err = ipfslog.NewLog(ipfs, identities[1], &ipfslog.LogOptions{ID: "X"})
			c.So(err, ShouldBeNil)

			log3, err = ipfslog.NewLog(ipfs, identities[2], &ipfslog.LogOptions{ID: "X"})
			c.So(err, ShouldBeNil)

			_, err = log2.Append(ctx, []byte("helloB1"), nil)
			c.So(err, ShouldBeNil)

			_, err = log2.Append(ctx, []byte("helloB2"), nil)
			c.So(err, ShouldBeNil)

			_, err = log3.Append(ctx, []byte("helloC1"), nil)
			c.So(err, ShouldBeNil)

			_, err = log3.Append(ctx, []byte("helloC2"), nil)
			c.So(err, ShouldBeNil)

			_, err = log3.Join(log2, -1)
			c.So(err, ShouldBeNil)

			resD1 := log3.ToString(nil)

			log2, err = ipfslog.NewLog(ipfs, identities[1], &ipfslog.LogOptions{ID: "X"})
			c.So(err, ShouldBeNil)

			log3, err = ipfslog.NewLog(ipfs, identities[2], &ipfslog.LogOptions{ID: "X"})
			c.So(err, ShouldBeNil)

			_, err = log2.Append(ctx, []byte("helloB1"), nil)
			c.So(err, ShouldBeNil)

			_, err = log2.Append(ctx, []byte("helloB2"), nil)
			c.So(err, ShouldBeNil)

			_, err = log3.Append(ctx, []byte("helloC1"), nil)
			c.So(err, ShouldBeNil)

			_, err = log3.Append(ctx, []byte("helloC2"), nil)
			c.So(err, ShouldBeNil)

			_, err = log2.Join(log3, -1)
			c.So(err, ShouldBeNil)

			resD2 := log2.ToString(nil)

			c.So(resD1, ShouldEqual, resD2)

			// a + b + c == c + b + a
			log1, err = ipfslog.NewLog(ipfs, identities[0], &ipfslog.LogOptions{ID: "X"})
			c.So(err, ShouldBeNil)

			log2, err = ipfslog.NewLog(ipfs, identities[1], &ipfslog.LogOptions{ID: "X"})
			c.So(err, ShouldBeNil)

			log3, err = ipfslog.NewLog(ipfs, identities[2], &ipfslog.LogOptions{ID: "X"})
			c.So(err, ShouldBeNil)

			_, err = log1.Append(ctx, []byte("helloA1"), nil)
			c.So(err, ShouldBeNil)

			_, err = log1.Append(ctx, []byte("helloA2"), nil)
			c.So(err, ShouldBeNil)

			_, err = log2.Append(ctx, []byte("helloB1"), nil)
			c.So(err, ShouldBeNil)

			_, err = log2.Append(ctx, []byte("helloB2"), nil)
			c.So(err, ShouldBeNil)

			_, err = log3.Append(ctx, []byte("helloC1"), nil)
			c.So(err, ShouldBeNil)

			_, err = log3.Append(ctx, []byte("helloC2"), nil)
			c.So(err, ShouldBeNil)

			_, err = log1.Join(log2, -1)
			c.So(err, ShouldBeNil)

			_, err = log1.Join(log3, -1)
			c.So(err, ShouldBeNil)

			logLeft := log1.ToString(nil)

			log1, err = ipfslog.NewLog(ipfs, identities[0], &ipfslog.LogOptions{ID: "X"})
			c.So(err, ShouldBeNil)

			log2, err = ipfslog.NewLog(ipfs, identities[1], &ipfslog.LogOptions{ID: "X"})
			c.So(err, ShouldBeNil)

			log3, err = ipfslog.NewLog(ipfs, identities[2], &ipfslog.LogOptions{ID: "X"})
			c.So(err, ShouldBeNil)

			_, err = log1.Append(ctx, []byte("helloA1"), nil)
			c.So(err, ShouldBeNil)

			_, err = log1.Append(ctx, []byte("helloA2"), nil)
			c.So(err, ShouldBeNil)

			_, err = log2.Append(ctx, []byte("helloB1"), nil)
			c.So(err, ShouldBeNil)

			_, err = log2.Append(ctx, []byte("helloB2"), nil)
			c.So(err, ShouldBeNil)

			_, err = log3.Append(ctx, []byte("helloC1"), nil)
			c.So(err, ShouldBeNil)

			_, err = log3.Append(ctx, []byte("helloC2"), nil)
			c.So(err, ShouldBeNil)

			_, err = log3.Join(log2, -1)
			c.So(err, ShouldBeNil)

			_, err = log3.Join(log1, -1)
			c.So(err, ShouldBeNil)

			logRight := log3.ToString(nil)

			c.So(logLeft, ShouldEqual, logRight)
		})

		Convey("join is idempotent", FailureHalts, func(c C) {
			expectedElementsCount := 3

			logA, err := ipfslog.NewLog(ipfs, identities[0], &ipfslog.LogOptions{ID: "X"})
			c.So(err, ShouldBeNil)

			_, err = logA.Append(ctx, []byte("helloA1"), nil)
			c.So(err, ShouldBeNil)

			_, err = logA.Append(ctx, []byte("helloA2"), nil)
			c.So(err, ShouldBeNil)

			_, err = logA.Append(ctx, []byte("helloA3"), nil)
			c.So(err, ShouldBeNil)

			// idempotence: a + a = a
			_, err = logA.Join(logA, -1)
			c.So(logA.Entries.Len(), ShouldEqual, expectedElementsCount)
		})
	})
}
