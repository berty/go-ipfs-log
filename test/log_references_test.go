package test

import (
	"context"
	"fmt"
	"math"
	"testing"

	ipfslog "berty.tech/go-ipfs-log"
	idp "berty.tech/go-ipfs-log/identityprovider"
	ks "berty.tech/go-ipfs-log/keystore"
	dssync "github.com/ipfs/go-datastore/sync"
	mocknet "github.com/libp2p/go-libp2p/p2p/net/mock"
	"github.com/stretchr/testify/require"
)

func TestLogReferences(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	m := mocknet.New(ctx)
	ipfs, closeNode := NewMemoryServices(ctx, t, m)
	defer closeNode()

	datastore := dssync.MutexWrap(NewIdentityDataStore(t))
	keystore, err := ks.NewKeystore(datastore)
	require.NoError(t, err)

	identity, err := idp.CreateIdentity(&idp.CreateIdentityOptions{
		Keystore: keystore,
		ID:       "userA",
		Type:     "orbitdb",
	})
	require.NoError(t, err)

	t.Run("creates entries with references", func(t *testing.T) {
		amount := 64
		maxReferenceDistance := 2

		log1, err := ipfslog.NewLog(ipfs, identity, &ipfslog.LogOptions{ID: "A"})
		require.NoError(t, err)

		log2, err := ipfslog.NewLog(ipfs, identity, &ipfslog.LogOptions{ID: "B"})
		require.NoError(t, err)

		log3, err := ipfslog.NewLog(ipfs, identity, &ipfslog.LogOptions{ID: "C"})
		require.NoError(t, err)

		log4, err := ipfslog.NewLog(ipfs, identity, &ipfslog.LogOptions{ID: "D"})
		require.NoError(t, err)

		for i := 0; i < amount; i++ {
			_, err := log1.Append(ctx, []byte(fmt.Sprintf("%d", i)), &ipfslog.AppendOptions{PointerCount: maxReferenceDistance})
			require.NoError(t, err)
		}

		for i := 0; i < amount*2; i++ {
			pointerCount := math.Pow(float64(maxReferenceDistance), 2)

			_, err := log2.Append(ctx, []byte(fmt.Sprintf("%d", i)), &ipfslog.AppendOptions{PointerCount: int(pointerCount)})
			require.NoError(t, err)
		}

		for i := 0; i < amount*3; i++ {
			pointerCount := math.Pow(float64(maxReferenceDistance), 3)

			_, err := log3.Append(ctx, []byte(fmt.Sprintf("%d", i)), &ipfslog.AppendOptions{PointerCount: int(pointerCount)})
			require.NoError(t, err)
		}

		for i := 0; i < amount*4; i++ {
			pointerCount := math.Pow(float64(maxReferenceDistance), 4)

			_, err := log4.Append(ctx, []byte(fmt.Sprintf("%d", i)), &ipfslog.AppendOptions{PointerCount: int(pointerCount)})
			require.NoError(t, err)
		}

		require.NotNil(t, log1.Entries.At(uint(log1.Entries.Len()-1)))
		require.Equal(t, len(log1.Entries.At(uint(log1.Entries.Len()-1)).GetNext()), 1)

		require.NotNil(t, log2.Entries.At(uint(log2.Entries.Len()-1)))
		require.Equal(t, len(log2.Entries.At(uint(log2.Entries.Len()-1)).GetNext()), 1)

		require.NotNil(t, log3.Entries.At(uint(log3.Entries.Len()-1)))
		require.Equal(t, len(log3.Entries.At(uint(log3.Entries.Len()-1)).GetNext()), 1)

		require.NotNil(t, log4.Entries.At(uint(log4.Entries.Len()-1)))
		require.Equal(t, len(log4.Entries.At(uint(log4.Entries.Len()-1)).GetNext()), 1)

		require.NotNil(t, log1.Entries.At(uint(log1.Entries.Len()-1)))
		require.Equal(t, len(log1.Entries.At(uint(log1.Entries.Len()-1)).GetRefs()), 1)

		require.NotNil(t, log2.Entries.At(uint(log2.Entries.Len()-1)))
		require.Equal(t, len(log2.Entries.At(uint(log2.Entries.Len()-1)).GetRefs()), 2)

		require.NotNil(t, log3.Entries.At(uint(log3.Entries.Len()-1)))
		require.Equal(t, len(log3.Entries.At(uint(log3.Entries.Len()-1)).GetRefs()), 3)

		require.NotNil(t, log4.Entries.At(uint(log4.Entries.Len()-1)))
		require.Equal(t, len(log4.Entries.At(uint(log4.Entries.Len()-1)).GetRefs()), 4)
	})
}

func TestLogReferences2(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	m := mocknet.New(ctx)
	ipfs, closeNode := NewMemoryServices(ctx, t, m)
	defer closeNode()

	datastore := dssync.MutexWrap(NewIdentityDataStore(t))
	keystore, err := ks.NewKeystore(datastore)
	require.NoError(t, err)

	identity, err := idp.CreateIdentity(&idp.CreateIdentityOptions{
		Keystore: keystore,
		ID:       "userA",
		Type:     "orbitdb",
	})
	require.NoError(t, err)

	for _, input := range []struct {
		amount         int
		referenceCount int
		refLength      int
	}{
		{1, 1, 0},
		{1, 2, 0},
		{2, 1, 1},
		{2, 2, 1},
		{3, 2, 1},
		{3, 4, 1},
		{4, 4, 2},
		{4, 4, 2},
		{32, 4, 2},
		{32, 8, 3},
		{32, 16, 4},
		{18, 32, 5},
		{128, 32, 5},
		{64, 64, 6},
		{65, 64, 6},
		{128, 64, 6},
		{128, 1, 0},
		{128, 2, 1},
		{256, 1, 0},
		{256, 256, 8},
		{256, 1024, 8},
	} {
		key := fmt.Sprintf("has %d references, max distance %d, total of %d entries", input.refLength, input.referenceCount, input.amount)
		t.Run(key, func(t *testing.T) {
			log1, err := ipfslog.NewLog(ipfs, identity, &ipfslog.LogOptions{ID: "A"})
			require.NoError(t, err)

			for i := 0; i < input.amount; i++ {
				_, err := log1.Append(ctx, []byte(fmt.Sprintf("%d", i+1)), &ipfslog.AppendOptions{PointerCount: input.referenceCount})
				require.NoError(t, err)
			}
			require.Equal(t, log1.Entries.Len(), input.amount)
			require.Equal(t, log1.Entries.At(uint(input.amount-1)).GetClock().GetTime(), input.amount)

			for k := 0; k < input.amount; k++ {
				idx := log1.Entries.Len() - k - 1

				atIdx := log1.Entries.At(uint(idx))
				require.NotNilf(t, atIdx, "idx=%d", idx)
				require.Equal(t, atIdx.GetClock().GetTime(), idx+1)

				// Check the first ref (distance 2)
				if len(atIdx.GetRefs()) > 0 {
					otherAt := log1.Entries.At(uint(idx - 2))
					require.NotNilf(t, otherAt, "index=%d", idx-2)
					require.True(t, atIdx.GetRefs()[0].Equals(otherAt.GetHash()))
				}

				// Check the second ref (distance 2)
				if len(atIdx.GetRefs()) > 1 && idx > input.referenceCount {
					otherAt := log1.Entries.At(uint(idx - 4))
					require.NotNilf(t, otherAt, "index=%d", idx-4)
					require.True(t, atIdx.GetRefs()[1].Equals(otherAt.GetHash()))
				}

				// Check the third ref (distance 4)
				if len(atIdx.GetRefs()) > 2 && idx > input.referenceCount {
					otherAt := log1.Entries.At(uint(idx - 8))
					require.NotNilf(t, otherAt, "index=%d", idx-8)
					require.True(t, atIdx.GetRefs()[2].Equals(otherAt.GetHash()))
				}

				// Check the fourth ref (distance 8)
				if len(atIdx.GetRefs()) > 3 && idx > input.referenceCount {
					otherAt := log1.Entries.At(uint(idx - 16))
					require.NotNilf(t, otherAt, "index=%d", idx-16)
					require.True(t, atIdx.GetRefs()[3].Equals(otherAt.GetHash()))
				}

				// Check the fifth ref (distance 16)
				if len(atIdx.GetRefs()) > 4 && idx > input.referenceCount {
					otherAt := log1.Entries.At(uint(idx - 32))
					require.NotNilf(t, otherAt, "index=%d", idx-32)
					require.True(t, atIdx.GetRefs()[4].Equals(otherAt.GetHash()))
				}

				// Check the reference of each entry
				if idx > input.referenceCount {
					require.Equal(t, len(atIdx.GetRefs()), input.refLength)
				}
			}
		})
	}
}
