package test

import (
	"context"
	"fmt"
	"math"
	"testing"

	ipfslog "berty.tech/go-ipfs-log"
	"berty.tech/go-ipfs-log/entry"
	idp "berty.tech/go-ipfs-log/identityprovider"
	"berty.tech/go-ipfs-log/iface"
	"berty.tech/go-ipfs-log/keystore"
	dssync "github.com/ipfs/go-datastore/sync"
	mocknet "github.com/libp2p/go-libp2p/p2p/net/mock"
	"github.com/stretchr/testify/require"
)

func TestLogAppend(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	m := mocknet.New(ctx)
	ipfs, closeNode := NewMemoryServices(ctx, t, m)
	defer closeNode()

	datastore := dssync.MutexWrap(NewIdentityDataStore(t))
	keystore, err := keystore.NewKeystore(datastore)
	require.NoError(t, err)

	identity, err := idp.CreateIdentity(&idp.CreateIdentityOptions{
		Keystore: keystore,
		ID:       fmt.Sprintf("userA"),
		Type:     "orbitdb",
	})
	require.NoError(t, err)

	t.Run("append one", func(t *testing.T) {
		log1, err := ipfslog.NewLog(ipfs, identity, &ipfslog.LogOptions{ID: "A"})
		require.NoError(t, err)
		_, err = log1.Append(ctx, []byte("hello1"), nil)
		require.NoError(t, err)

		require.Equal(t, log1.Entries.Len(), 1)
		values := log1.Values()
		keys := values.Keys()

		for _, k := range keys {
			v := values.UnsafeGet(k)
			require.Equal(t, string(v.GetPayload()), "hello1")
			require.Equal(t, len(v.GetNext()), 0)
			require.Equal(t, v.GetClock().GetID(), identity.PublicKey)
			require.Equal(t, v.GetClock().GetTime(), 1)
		}
		for _, v := range entry.FindHeads(log1.Entries) {
			require.Equal(t, v.GetHash().String(), values.UnsafeGet(keys[0]).GetHash().String())
		}
	})

	t.Run("append 100 items to a log", func(t *testing.T) {
		log1, err := ipfslog.NewLog(ipfs, identity, &ipfslog.LogOptions{ID: "A"})
		require.NoError(t, err)
		nextPointerAmount := 64

		for i := 0; i < 100; i++ {
			_, err := log1.Append(ctx, []byte(fmt.Sprintf("hello%d", i)), &iface.AppendOptions{
				PointerCount: nextPointerAmount,
			})
			require.NoError(t, err)

			values := log1.Values()
			keys := values.Keys()
			heads := entry.FindHeads(log1.Entries)

			require.Equal(t, len(heads), 1)
			require.Equal(t, heads[0].GetHash().String(), values.UnsafeGet(keys[len(keys)-1]).GetHash().String())
		}

		require.Equal(t, log1.Entries.Len(), 100)

		values := log1.Values()
		keys := values.Keys()

		for i, k := range keys {
			v := values.UnsafeGet(k)

			require.Equal(t, string(v.GetPayload()), fmt.Sprintf("hello%d", i))
			require.Equal(t, v.GetClock().GetTime(), i+1)
			require.Equal(t, v.GetClock().GetID(), identity.PublicKey)

			if i == 0 {
				require.Equal(t, len(v.GetRefs()), 0)
			} else {
				expected := math.Ceil(math.Log2(math.Min(float64(nextPointerAmount), float64(i))))

				require.Equal(t, len(v.GetRefs()), int(expected))
			}
		}
	})
}
