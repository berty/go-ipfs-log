package test

import (
	"berty.tech/go-ipfs-log/enc"
	"berty.tech/go-ipfs-log/io/cbor"
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

	m := mocknet.New()
	ipfs, closeNode := NewMemoryServices(ctx, t, m)
	defer closeNode()

	datastore := dssync.MutexWrap(NewIdentityDataStore(t))
	keystore, err := keystore.NewKeystore(datastore)
	require.NoError(t, err)

	identity, err := idp.CreateIdentity(ctx, &idp.CreateIdentityOptions{
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

func TestLogAppendEncrypted(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	m := mocknet.New()
	ipfs, closeNode := NewMemoryServices(ctx, t, m)
	defer closeNode()

	datastore := dssync.MutexWrap(NewIdentityDataStore(t))
	keystore, err := keystore.NewKeystore(datastore)
	require.NoError(t, err)

	identity, err := idp.CreateIdentity(ctx, &idp.CreateIdentityOptions{
		Keystore: keystore,
		ID:       fmt.Sprintf("userA"),
		Type:     "orbitdb",
	})
	require.NoError(t, err)

	cborioDefault, err := cbor.IO(&entry.Entry{}, &entry.LamportClock{})
	require.NoError(t, err)

	logKey, err := enc.NewSecretbox([]byte{
		'0', '1', '2', '3', '4', '5', '6', '7', '8', '9',
		0, 1, 2, 3, 4, 5, 6, 7, 8, 9,
		'0', '1', '2', '3', '4', '5', '6', '7', '8', '9',
		'a', 'b',
	})
	require.NoError(t, err)

	logKeyDiff, err := enc.NewSecretbox([]byte{
		'0', '1', '2', '3', '4', '5', '6', '7', '8', '9',
		0, 1, 2, 3, 4, 5, 6, 7, 8, 9,
		'0', '1', '2', '3', '4', '5', '6', '7', '8', '9',
		'a', 'c',
	})
	require.NoError(t, err)

	cborioDiff := cborioDefault.ApplyOptions(&cbor.Options{LinkKey: logKeyDiff})
	cborio := cborioDefault.ApplyOptions(&cbor.Options{LinkKey: logKey})

	t.Run("NewFromEntryHash - succeed with same keys", func(t *testing.T) {
		l, err := ipfslog.NewLog(ipfs, identity, &ipfslog.LogOptions{ID: "X", IO: cborio})
		require.NoError(t, err)

		_, err = l.Append(ctx, []byte("helloA1"), nil)
		require.NoError(t, err)

		_, err = l.Append(ctx, []byte("helloA2"), nil)
		require.NoError(t, err)

		_, err = l.Append(ctx, []byte("helloA3"), nil)
		require.NoError(t, err)

		h, err := l.Append(ctx, []byte("helloA4"), nil)
		require.NoError(t, err)

		l2, err := ipfslog.NewFromEntryHash(ctx, ipfs, identity, h.GetHash(),
			&ipfslog.LogOptions{
				ID: "A",
				IO: cborio,
			}, &ipfslog.FetchOptions{})
		require.NoError(t, err)

		require.Equal(t, 4, l.Values().Len())
		//require.Equal(t, 4, l2.Values().Len())

		expected := []string{"helloA1", "helloA2", "helloA3", "helloA4"}
		var result []string

		for _, v := range l2.Values().Keys() {
			result = append(result, string(l2.Values().UnsafeGet(v).GetPayload()))
		}

		require.Equal(t, expected, result)
		require.Equal(t, len(getLastEntry(l2.Values()).GetNext()), 1)
	})

	t.Run("NewFromEntryHash - fails with diff keys", func(t *testing.T) {
		l, err := ipfslog.NewLog(ipfs, identity, &ipfslog.LogOptions{ID: "X", IO: cborio})
		require.NoError(t, err)

		_, err = l.Append(ctx, []byte("helloA1"), nil)
		require.NoError(t, err)

		_, err = l.Append(ctx, []byte("helloA2"), nil)
		require.NoError(t, err)

		_, err = l.Append(ctx, []byte("helloA3"), nil)
		require.NoError(t, err)

		h, err := l.Append(ctx, []byte("helloA4"), nil)
		require.NoError(t, err)

		l2, err := ipfslog.NewFromEntryHash(ctx, ipfs, identity, h.GetHash(),
			&ipfslog.LogOptions{
				ID: "A",
				IO: cborioDiff,
			}, &ipfslog.FetchOptions{})
		require.NoError(t, err)

		require.Equal(t, 4, l.Values().Len())
		require.Equal(t, 0, l2.Values().Len())

		var result []string
		for _, v := range l2.Values().Keys() {
			result = append(result, string(l2.Values().UnsafeGet(v).GetPayload()))
		}

		require.Empty(t, result)
	})

	t.Run("NewFromEntryHash - fails with no key", func(t *testing.T) {
		l, err := ipfslog.NewLog(ipfs, identity, &ipfslog.LogOptions{ID: "X", IO: cborio})
		require.NoError(t, err)

		_, err = l.Append(ctx, []byte("helloA1"), nil)
		require.NoError(t, err)

		_, err = l.Append(ctx, []byte("helloA2"), nil)
		require.NoError(t, err)

		_, err = l.Append(ctx, []byte("helloA3"), nil)
		require.NoError(t, err)

		h, err := l.Append(ctx, []byte("helloA4"), nil)
		require.NoError(t, err)

		l2, err := ipfslog.NewFromEntryHash(ctx, ipfs, identity, h.GetHash(),
			&ipfslog.LogOptions{
				ID: "A",
				IO: cborioDefault,
			}, &ipfslog.FetchOptions{})
		require.NoError(t, err)

		require.Equal(t, 4, l.Values().Len())
		require.Equal(t, 1, l2.Values().Len())

		var result []string
		for _, v := range l2.Values().Keys() {
			result = append(result, string(l2.Values().UnsafeGet(v).GetPayload()))
		}

		require.Equal(t, result, []string{"helloA4"})
	})
}
