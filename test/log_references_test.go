package test

import (
	"context"
	"fmt"
	"math"
	"testing"
	"time"

	dssync "github.com/ipfs/go-datastore/sync"
	mocknet "github.com/libp2p/go-libp2p/p2p/net/mock"
	. "github.com/smartystreets/goconvey/convey"

	ipfslog "berty.tech/go-ipfs-log"
	idp "berty.tech/go-ipfs-log/identityprovider"
	ks "berty.tech/go-ipfs-log/keystore"
)

func TestLogReferences(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*10)
	defer cancel()

	m := mocknet.New(ctx)
	ipfs, closeNode := NewMemoryServices(ctx, t, m)
	defer closeNode()

	datastore := dssync.MutexWrap(NewIdentityDataStore(t))
	keystore, err := ks.NewKeystore(datastore)
	if err != nil {
		t.Fatal(err)
	}

	identity, err := idp.CreateIdentity(&idp.CreateIdentityOptions{
		Keystore: keystore,
		ID:       "userA",
		Type:     "orbitdb",
	})
	if err != nil {
		t.Fatal(err)
	}

	Convey("References", t, FailureContinues, func(c C) {
		Convey("creates entries with references", func(c C) {
			amount := 64
			maxReferenceDistance := 2
			log1, err := ipfslog.NewLog(ipfs, identity, &ipfslog.LogOptions{ID: "A"})
			c.So(err, ShouldBeNil)

			log2, err := ipfslog.NewLog(ipfs, identity, &ipfslog.LogOptions{ID: "B"})
			c.So(err, ShouldBeNil)

			log3, err := ipfslog.NewLog(ipfs, identity, &ipfslog.LogOptions{ID: "C"})
			c.So(err, ShouldBeNil)

			log4, err := ipfslog.NewLog(ipfs, identity, &ipfslog.LogOptions{ID: "D"})
			c.So(err, ShouldBeNil)

			for i := 0; i < amount; i++ {
				_, err := log1.Append(ctx, []byte(fmt.Sprintf("%d", i)), &ipfslog.AppendOptions{
					PointerCount: maxReferenceDistance,
				})
				c.So(err, ShouldBeNil)
			}

			for i := 0; i < amount*2; i++ {
				pointerCount := math.Pow(float64(maxReferenceDistance), 2)

				_, err := log2.Append(ctx, []byte(fmt.Sprintf("%d", i)), &ipfslog.AppendOptions{
					PointerCount: int(pointerCount),
				})
				c.So(err, ShouldBeNil)
			}

			for i := 0; i < amount*3; i++ {
				pointerCount := math.Pow(float64(maxReferenceDistance), 3)

				_, err := log3.Append(ctx, []byte(fmt.Sprintf("%d", i)), &ipfslog.AppendOptions{
					PointerCount: int(pointerCount),
				})
				c.So(err, ShouldBeNil)
			}

			for i := 0; i < amount*4; i++ {
				pointerCount := math.Pow(float64(maxReferenceDistance), 4)

				_, err := log4.Append(ctx, []byte(fmt.Sprintf("%d", i)), &ipfslog.AppendOptions{
					PointerCount: int(pointerCount),
				})
				c.So(err, ShouldBeNil)
			}

			c.So(log1.Entries.At(uint(log1.Entries.Len()-1)), ShouldNotBeNil)
			c.So(len(log1.Entries.At(uint(log1.Entries.Len()-1)).GetNext()), ShouldEqual, 1)

			c.So(log2.Entries.At(uint(log2.Entries.Len()-1)), ShouldNotBeNil)
			c.So(len(log2.Entries.At(uint(log2.Entries.Len()-1)).GetNext()), ShouldEqual, 1)

			c.So(log3.Entries.At(uint(log3.Entries.Len()-1)), ShouldNotBeNil)
			c.So(len(log3.Entries.At(uint(log3.Entries.Len()-1)).GetNext()), ShouldEqual, 1)

			c.So(log4.Entries.At(uint(log4.Entries.Len()-1)), ShouldNotBeNil)
			c.So(len(log4.Entries.At(uint(log4.Entries.Len()-1)).GetNext()), ShouldEqual, 1)

			c.So(log1.Entries.At(uint(log1.Entries.Len()-1)), ShouldNotBeNil)
			c.So(len(log1.Entries.At(uint(log1.Entries.Len()-1)).GetRefs()), ShouldEqual, 1)

			c.So(log2.Entries.At(uint(log2.Entries.Len()-1)), ShouldNotBeNil)
			c.So(len(log2.Entries.At(uint(log2.Entries.Len()-1)).GetRefs()), ShouldEqual, 2)

			c.So(log3.Entries.At(uint(log3.Entries.Len()-1)), ShouldNotBeNil)
			c.So(len(log3.Entries.At(uint(log3.Entries.Len()-1)).GetRefs()), ShouldEqual, 3)

			c.So(log4.Entries.At(uint(log4.Entries.Len()-1)), ShouldNotBeNil)
			c.So(len(log4.Entries.At(uint(log4.Entries.Len()-1)).GetRefs()), ShouldEqual, 4)
		})
	})
}

func TestLogReferences2(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*10)
	defer cancel()

	m := mocknet.New(ctx)
	ipfs, closeNode := NewMemoryServices(ctx, t, m)
	defer closeNode()

	datastore := dssync.MutexWrap(NewIdentityDataStore(t))
	keystore, err := ks.NewKeystore(datastore)
	if err != nil {
		t.Fatal(err)
	}

	identity, err := idp.CreateIdentity(&idp.CreateIdentityOptions{
		Keystore: keystore,
		ID:       "userA",
		Type:     "orbitdb",
	})
	if err != nil {
		t.Fatal(err)
	}

	for _, input := range []struct {
		amount         int
		referenceCount int
		refLength      int
	}{
		{amount: 1, referenceCount: 1, refLength: 0},
		{amount: 1, referenceCount: 2, refLength: 0},
		{amount: 2, referenceCount: 1, refLength: 1},
		{amount: 2, referenceCount: 2, refLength: 1},
		{amount: 3, referenceCount: 2, refLength: 1},
		{amount: 3, referenceCount: 4, refLength: 1},
		{amount: 4, referenceCount: 4, refLength: 2},
		{amount: 4, referenceCount: 4, refLength: 2},
		{amount: 32, referenceCount: 4, refLength: 2},
		{amount: 32, referenceCount: 8, refLength: 3},
		{amount: 32, referenceCount: 16, refLength: 4},
		{amount: 18, referenceCount: 32, refLength: 5},
		{amount: 128, referenceCount: 32, refLength: 5},
		{amount: 64, referenceCount: 64, refLength: 6},
		{amount: 65, referenceCount: 64, refLength: 6},
		{amount: 128, referenceCount: 64, refLength: 6},
		{amount: 128, referenceCount: 1, refLength: 0},
		{amount: 128, referenceCount: 2, refLength: 1},
		{amount: 256, referenceCount: 1, refLength: 0},
		{amount: 256, referenceCount: 256, refLength: 8},
		{amount: 256, referenceCount: 1024, refLength: 8},
	} {
		t.Run(fmt.Sprintf("has %d references, max distance %d, total of %d entries", input.refLength, input.referenceCount, input.amount), func(t *testing.T) {
			log1, err := ipfslog.NewLog(ipfs, identity, &ipfslog.LogOptions{ID: "A"})
			if err != nil {
				t.Fatal(err)
			}

			for i := 0; i < input.amount; i++ {
				_, err := log1.Append(ctx, []byte(fmt.Sprintf("%d", i+1)), &ipfslog.AppendOptions{PointerCount: input.referenceCount})
				if err != nil {
					t.Fatal(err)
				}
			}

			if log1.Entries.Len() != input.amount {
				t.Fatalf("%d != %d", log1.Entries.Len(), input.amount)
			}

			if clockTime := log1.Entries.At(uint(input.amount - 1)).GetClock().GetTime(); clockTime != input.amount {
				t.Fatalf("%d != %d", clockTime, input.amount)
			}

			for k := 0; k < input.amount; k++ {
				idx := log1.Entries.Len() - k - 1

				atIdx := log1.Entries.At(uint(idx))
				if atIdx == nil {
					t.Fatalf("value at index %d should not be nil", idx)
				}

				if clockTime := atIdx.GetClock().GetTime(); clockTime != idx+1 {
					t.Fatalf("%d != %d", clockTime, idx+1)
				}

				// Check the first ref (distance 2)
				if len(atIdx.GetRefs()) > 0 {
					otherAt := log1.Entries.At(uint(idx - 2))
					if otherAt == nil {
						t.Fatalf("value at index %d should not be nil", idx-2)
					}

					if !atIdx.GetRefs()[0].Equals(otherAt.GetHash()) {
						t.Fatalf("hashes should be equal")
					}
				}

				// Check the second ref (distance 2)

				if len(atIdx.GetRefs()) > 1 && idx > input.referenceCount {
					otherAt := log1.Entries.At(uint(idx - 4))
					if otherAt == nil {
						t.Fatalf("value at index %d should not be nil", idx-4)
					}

					if !atIdx.GetRefs()[1].Equals(otherAt.GetHash()) {
						t.Fatalf("hashes should be equal")
					}
				}

				// Check the third ref (distance 4)
				if len(atIdx.GetRefs()) > 2 && idx > input.referenceCount {
					otherAt := log1.Entries.At(uint(idx - 8))
					if otherAt == nil {
						t.Fatalf("value at index %d should not be nil", idx-8)
					}

					if !atIdx.GetRefs()[2].Equals(otherAt.GetHash()) {
						t.Fatalf("hashes should be equal")
					}
				}

				// Check the fourth ref (distance 8)
				if len(atIdx.GetRefs()) > 3 && idx > input.referenceCount {
					otherAt := log1.Entries.At(uint(idx - 16))
					if otherAt == nil {
						t.Fatalf("value at index %d should not be nil", idx-16)
					}

					if !atIdx.GetRefs()[3].Equals(otherAt.GetHash()) {
						t.Fatalf("hashes should be equal")
					}
				}

				// Check the fifth ref (distance 16)
				if len(atIdx.GetRefs()) > 4 && idx > input.referenceCount {
					otherAt := log1.Entries.At(uint(idx - 32))
					if otherAt == nil {
						t.Fatalf("value at index %d should not be nil", idx-32)
					}

					if !atIdx.GetRefs()[4].Equals(otherAt.GetHash()) {
						t.Fatalf("hashes should be equal")
					}
				}

				// Check the reference of each entry
				if idx > input.referenceCount && len(atIdx.GetRefs()) != input.refLength {
					t.Fatalf("invalid value for ref length")
				}
			}
		})
	}
}
