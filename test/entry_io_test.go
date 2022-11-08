package test

import (
	"context"
	"fmt"
	"testing"

	ipfslog "berty.tech/go-ipfs-log"
	"berty.tech/go-ipfs-log/entry"
	idp "berty.tech/go-ipfs-log/identityprovider"
	"berty.tech/go-ipfs-log/iface"
	ks "berty.tech/go-ipfs-log/keystore"
	cid "github.com/ipfs/go-cid"
	dssync "github.com/ipfs/go-datastore/sync"
	mocknet "github.com/libp2p/go-libp2p/p2p/net/mock"
	"github.com/stretchr/testify/require"
)

func TestEntryPersistence(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	m := mocknet.New()
defer m.Close()
	ipfs, closeNode := NewMemoryServices(ctx, t, m)
	defer closeNode()

	datastore := dssync.MutexWrap(NewIdentityDataStore(t))
	keystore, err := ks.NewKeystore(datastore)
	require.NoError(t, err)

	var identities []*idp.Identity

	for i := 0; i < 4; i++ {
		char := 'A' + i

		identity, err := idp.CreateIdentity(ctx, &idp.CreateIdentityOptions{
			Keystore: keystore,
			ID:       fmt.Sprintf("user%c", char),
			Type:     "orbitdb",
		})
		require.NoError(t, err)

		identities = append(identities, identity)
	}

	t.Run("log with 1 entry", func(t *testing.T) {
		log1, err := ipfslog.NewLog(ipfs, identities[0], &ipfslog.LogOptions{ID: "X"})
		require.NoError(t, err)
		e, err := log1.Append(ctx, []byte("one"), nil)
		require.NoError(t, err)

		hash := e.GetHash()
		res := entry.FetchAll(ctx, ipfs, []cid.Cid{hash}, &entry.FetchOptions{Length: intPtr(1)})
		require.Equal(t, len(res), 1)
	})

	t.Run("log with 2 entries", func(t *testing.T) {
		log1, err := ipfslog.NewLog(ipfs, identities[0], &ipfslog.LogOptions{ID: "X"})
		require.NoError(t, err)
		_, err = log1.Append(ctx, []byte("one"), nil)
		require.NoError(t, err)
		e, err := log1.Append(ctx, []byte("two"), nil)
		require.NoError(t, err)

		hash := e.GetHash()
		res := entry.FetchAll(ctx, ipfs, []cid.Cid{hash}, &entry.FetchOptions{Length: intPtr(2)})
		require.Equal(t, len(res), 2)
	})

	t.Run("loads max 1 entry from a log of 2 entries", func(t *testing.T) {
		log1, err := ipfslog.NewLog(ipfs, identities[0], &ipfslog.LogOptions{ID: "X"})
		require.NoError(t, err)
		_, err = log1.Append(ctx, []byte("one"), nil)
		require.NoError(t, err)
		e, err := log1.Append(ctx, []byte("two"), nil)
		require.NoError(t, err)

		hash := e.GetHash()
		res := entry.FetchAll(ctx, ipfs, []cid.Cid{hash}, &entry.FetchOptions{Length: intPtr(1)})
		require.Equal(t, len(res), 1)
	})

	t.Run("log with 100 entries", func(t *testing.T) {
		var e iface.IPFSLogEntry
		var err error

		log1, err := ipfslog.NewLog(ipfs, identities[0], &ipfslog.LogOptions{ID: "X"})
		require.NoError(t, err)
		for i := 0; i < 100; i++ {
			e, err = log1.Append(ctx, []byte(fmt.Sprintf("hello%d", i)), nil)
			require.NoError(t, err)
		}

		hash := e.GetHash()
		res := entry.FetchAll(ctx, ipfs, []cid.Cid{hash}, &entry.FetchOptions{})
		require.Equal(t, len(res), 100)
	})

	t.Run("load only 42 entries from a log with 100 entries", func(t *testing.T) {
		log1, err := ipfslog.NewLog(ipfs, identities[0], &ipfslog.LogOptions{ID: "X"})
		require.NoError(t, err)
		log2, err := ipfslog.NewLog(ipfs, identities[0], &ipfslog.LogOptions{ID: "X"})
		require.NoError(t, err)

		for i := 0; i < 100; i++ {
			_, err := log1.Append(ctx, []byte(fmt.Sprintf("hello%d", i)), nil)
			require.NoError(t, err)
			if i%10 == 0 {
				heads := append(entry.FindHeads(log2.Entries), entry.FindHeads(log1.Entries)...)
				log2, err = ipfslog.NewLog(ipfs, identities[0], &ipfslog.LogOptions{ID: log2.ID, Entries: log2.Values(), Heads: heads})
				require.NoError(t, err)
				_, err := log2.Append(ctx, []byte(fmt.Sprintf("hi%d", i)), nil)
				require.NoError(t, err)
			}
		}

		hash, err := log1.ToMultihash(ctx)
		require.NoError(t, err)

		res, err := ipfslog.NewFromMultihash(ctx, ipfs, identities[0], hash, &ipfslog.LogOptions{}, &ipfslog.FetchOptions{Length: intPtr(42)})
		require.NoError(t, err)
		require.Equal(t, res.Entries.Len(), 42)
	})

	t.Run("load only 99 entries from a log with 100 entries", func(t *testing.T) {
		log1, err := ipfslog.NewLog(ipfs, identities[0], &ipfslog.LogOptions{ID: "X"})
		require.NoError(t, err)
		log2, err := ipfslog.NewLog(ipfs, identities[0], &ipfslog.LogOptions{ID: "X"})
		require.NoError(t, err)

		for i := 0; i < 100; i++ {
			_, err := log1.Append(ctx, []byte(fmt.Sprintf("hello%d", i)), nil)
			require.NoError(t, err)
			if i%10 == 0 {
				log2, err = ipfslog.NewLog(ipfs, identities[0], &ipfslog.LogOptions{ID: log2.ID, Entries: log2.Values()})
				require.NoError(t, err)
				_, err := log2.Append(ctx, []byte(fmt.Sprintf("hi%d", i)), nil)
				require.NoError(t, err)
				_, err = log2.Join(log1, -1)
				require.NoError(t, err)
			}
		}

		hash, err := log2.ToMultihash(ctx)
		require.NoError(t, err)

		res, err := ipfslog.NewFromMultihash(ctx, ipfs, identities[0], hash, &ipfslog.LogOptions{}, &ipfslog.FetchOptions{Length: intPtr(99)})
		require.NoError(t, err)
		require.Equal(t, res.Entries.Len(), 99)
	})

	t.Run("load only 10 entries from a log with 100 entries", func(t *testing.T) {
		log1, err := ipfslog.NewLog(ipfs, identities[0], &ipfslog.LogOptions{ID: "X"})
		require.NoError(t, err)
		log2, err := ipfslog.NewLog(ipfs, identities[0], &ipfslog.LogOptions{ID: "X"})
		require.NoError(t, err)
		log3, err := ipfslog.NewLog(ipfs, identities[0], &ipfslog.LogOptions{ID: "X"})
		require.NoError(t, err)

		for i := 0; i < 100; i++ {
			_, err := log1.Append(ctx, []byte(fmt.Sprintf("hello%d", i)), nil)
			require.NoError(t, err)
			if i%10 == 0 {
				log2, err = ipfslog.NewLog(ipfs, identities[0], &ipfslog.LogOptions{ID: log2.ID, Entries: log2.Values(), Heads: entry.FindHeads(log2.Entries)})
				require.NoError(t, err)
				_, err := log2.Append(ctx, []byte(fmt.Sprintf("hi%d", i)), nil)
				require.NoError(t, err)
				_, err = log2.Join(log1, -1)
				require.NoError(t, err)
			}
			if i%25 == 0 {
				heads := append(entry.FindHeads(log3.Entries), entry.FindHeads(log2.Entries)...)
				log3, err = ipfslog.NewLog(ipfs, identities[0], &ipfslog.LogOptions{ID: log3.ID, Entries: log3.Values(), Heads: heads})
				require.NoError(t, err)
				_, err := log3.Append(ctx, []byte(fmt.Sprintf("--%d", i)), nil)
				require.NoError(t, err)
			}
		}

		_, err = log3.Join(log2, -1)
		require.NoError(t, err)

		hash, err := log3.ToMultihash(ctx)
		require.NoError(t, err)

		res, err := ipfslog.NewFromMultihash(ctx, ipfs, identities[0], hash, &ipfslog.LogOptions{}, &ipfslog.FetchOptions{Length: intPtr(10)})
		require.NoError(t, err)
		require.Equal(t, res.Entries.Len(), 10)
	})

	t.Run("load only 10 entries and then expand to max from a log with 100 entries", func(t *testing.T) {
		log1, err := ipfslog.NewLog(ipfs, identities[0], &ipfslog.LogOptions{ID: "X"})
		require.NoError(t, err)
		log2, err := ipfslog.NewLog(ipfs, identities[1], &ipfslog.LogOptions{ID: "X"})
		require.NoError(t, err)
		log3, err := ipfslog.NewLog(ipfs, identities[2], &ipfslog.LogOptions{ID: "X"})
		require.NoError(t, err)

		for i := 0; i < 30; i++ {
			_, err := log1.Append(ctx, []byte(fmt.Sprintf("hello%d", i)), nil)
			require.NoError(t, err)
			if i%10 == 0 {
				_, err := log2.Append(ctx, []byte(fmt.Sprintf("hi%d", i)), nil)
				require.NoError(t, err)
				_, err = log2.Join(log1, -1)
				require.NoError(t, err)
			}
			if i%25 == 0 {
				heads := append(entry.FindHeads(log3.Entries), entry.FindHeads(log2.Entries)...)
				log3, err = ipfslog.NewLog(ipfs, identities[2], &ipfslog.LogOptions{ID: log3.ID, Entries: log3.Values(), Heads: heads})
				require.NoError(t, err)
				_, err := log3.Append(ctx, []byte(fmt.Sprintf("--%d", i)), nil)
				require.NoError(t, err)
			}
		}

		_, err = log3.Join(log2, -1)
		require.NoError(t, err)

		log4, err := ipfslog.NewLog(ipfs, identities[3], &ipfslog.LogOptions{ID: "X"})
		require.NoError(t, err)
		_, err = log4.Join(log2, -1)
		require.NoError(t, err)
		_, err = log4.Join(log3, -1)
		require.NoError(t, err)

		var values3, values4 [][]byte
		log3Values := log3.Values()
		log3Keys := log3Values.Keys()

		log4Values := log4.Values()
		log4Keys := log4Values.Keys()

		for _, k := range log3Keys {
			v, _ := log3Values.Get(k)
			values3 = append(values3, v.GetPayload())
		}
		for _, k := range log4Keys {
			v, _ := log4Values.Get(k)
			values4 = append(values4, v.GetPayload())
		}
		require.Equal(t, values3, values4)
	})
}
