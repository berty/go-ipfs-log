package test

import (
	"context"
	"fmt"
	"testing"

	ipfslog "berty.tech/go-ipfs-log"
	"berty.tech/go-ipfs-log/entry/sorting"
	idp "berty.tech/go-ipfs-log/identityprovider"
	ks "berty.tech/go-ipfs-log/keystore"
	dssync "github.com/ipfs/go-datastore/sync"
	mocknet "github.com/libp2p/go-libp2p/p2p/net/mock"
	"github.com/stretchr/testify/require"
)

func TestLogJoinConcurrent(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	m := mocknet.New()
	ipfs, closeNode := NewMemoryServices(ctx, t, m)
	defer closeNode()

	datastore := dssync.MutexWrap(NewIdentityDataStore(t))
	keystore, err := ks.NewKeystore(datastore)
	require.NoError(t, err)

	t.Run("join", func(t *testing.T) {
		identity, err := idp.CreateIdentity(ctx, &idp.CreateIdentityOptions{
			Keystore: keystore,
			ID:       "userA",
			Type:     "orbitdb",
		})
		require.NoError(t, err)

		log1, err := ipfslog.NewLog(ipfs, identity, &ipfslog.LogOptions{ID: "A", SortFn: sorting.SortByEntryHash})
		require.NoError(t, err)

		log2, err := ipfslog.NewLog(ipfs, identity, &ipfslog.LogOptions{ID: "A", SortFn: sorting.SortByEntryHash})
		require.NoError(t, err)

		// joins consistently
		for i := 0; i < 10; i++ {
			_, err = log1.Append(ctx, []byte(fmt.Sprintf("hello1-%d", i)), nil)
			require.NoError(t, err)

			_, err = log2.Append(ctx, []byte(fmt.Sprintf("hello2-%d", i)), nil)
			require.NoError(t, err)
		}

		_, err = log1.Join(log2, -1)
		require.NoError(t, err)

		_, err = log2.Join(log1, -1)
		require.NoError(t, err)

		hash1, err := log1.ToMultihash(ctx)
		require.NoError(t, err)

		hash2, err := log2.ToMultihash(ctx)
		require.NoError(t, err)

		require.True(t, hash1.Equals(hash2))
		require.Equal(t, log1.Values().Len(), 20)
		require.Equal(t, log1.ToString(nil), log2.ToString(nil))

		// Concurrently appending same payload after join results in same state
		for i := 10; i < 20; i++ {
			_, err = log1.Append(ctx, []byte(fmt.Sprintf("hello1-%d", i)), nil)
			require.NoError(t, err)

			_, err = log2.Append(ctx, []byte(fmt.Sprintf("hello2-%d", i)), nil)
			require.NoError(t, err)
		}

		_, err = log1.Join(log2, -1)
		require.NoError(t, err)

		_, err = log2.Join(log1, -1)
		require.NoError(t, err)

		_, err = log1.Append(ctx, []byte("same"), nil)
		require.NoError(t, err)

		_, err = log2.Append(ctx, []byte("same"), nil)
		require.NoError(t, err)

		hash1, err = log1.ToMultihash(ctx)
		require.NoError(t, err)

		hash2, err = log2.ToMultihash(ctx)
		require.NoError(t, err)

		require.True(t, hash1.Equals(hash2))
		require.Equal(t, log1.Values().Len(), 41)
		require.Equal(t, log2.Values().Len(), 41)
		require.Equal(t, log1.ToString(nil), log2.ToString(nil))

		// Joining after concurrently appending same payload joins entry once
		_, err = log1.Join(log2, -1)
		require.NoError(t, err)

		_, err = log2.Join(log1, -1)
		require.NoError(t, err)

		require.Equal(t, log1.Entries.Len(), log2.Entries.Len())
		require.Equal(t, log1.Entries.Len(), 41)
		require.Equal(t, log1.ToString(nil), log2.ToString(nil))
	})
}
