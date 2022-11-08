package test

import (
	"context"
	"fmt"
	"testing"

	ipfslog "berty.tech/go-ipfs-log"
	"berty.tech/go-ipfs-log/entry"
	idp "berty.tech/go-ipfs-log/identityprovider"
	ks "berty.tech/go-ipfs-log/keystore"
	ds "github.com/ipfs/go-datastore"
	dssync "github.com/ipfs/go-datastore/sync"
	mocknet "github.com/libp2p/go-libp2p/p2p/net/mock"
	"github.com/stretchr/testify/require"
)

func TestLogHeadsTails(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	m := mocknet.New()
	defer m.Close()
	ipfs, closeNode := NewMemoryServices(ctx, t, m)
	defer closeNode()

	datastore := dssync.MutexWrap(ds.NewMapDatastore())
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

	t.Run("heads", func(t *testing.T) {
		t.Run("finds one head after one entry", func(t *testing.T) {
			log1, err := ipfslog.NewLog(ipfs, identities[0], &ipfslog.LogOptions{ID: "A"})
			require.NoError(t, err)
			_, err = log1.Append(ctx, []byte("helloA1"), nil)
			require.NoError(t, err)

			require.Equal(t, len(entry.FindHeads(log1.Entries)), 1)
		})

		t.Run("finds one head after two entry", func(t *testing.T) {
			log1, err := ipfslog.NewLog(ipfs, identities[0], &ipfslog.LogOptions{ID: "A"})
			require.NoError(t, err)
			_, err = log1.Append(ctx, []byte("helloA1"), nil)
			require.NoError(t, err)
			_, err = log1.Append(ctx, []byte("helloA2"), nil)
			require.NoError(t, err)

			require.Equal(t, len(entry.FindHeads(log1.Entries)), 1)
		})

		t.Run("finds head after a join and append", func(t *testing.T) {
			log1, err := ipfslog.NewLog(ipfs, identities[0], &ipfslog.LogOptions{ID: "A"})
			require.NoError(t, err)
			log2, err := ipfslog.NewLog(ipfs, identities[0], &ipfslog.LogOptions{ID: "A"})
			require.NoError(t, err)

			_, err = log1.Append(ctx, []byte("helloA1"), nil)
			require.NoError(t, err)
			_, err = log1.Append(ctx, []byte("helloA2"), nil)
			require.NoError(t, err)
			_, err = log2.Append(ctx, []byte("helloB1"), nil)
			require.NoError(t, err)

			_, err = log2.Join(log1, -1)
			require.NoError(t, err)
			_, err = log2.Append(ctx, []byte("helloB2"), nil)
			require.NoError(t, err)

			lastEntry := getLastEntry(log2.Values())

			require.Equal(t, len(entry.FindHeads(log2.Entries)), 1)
			require.Equal(t, entry.FindHeads(log2.Entries)[0].GetHash().String(), lastEntry.GetHash().String())
		})

		t.Run("finds two heads after a join", func(t *testing.T) {
			log1, err := ipfslog.NewLog(ipfs, identities[0], &ipfslog.LogOptions{ID: "A"})
			require.NoError(t, err)
			log2, err := ipfslog.NewLog(ipfs, identities[0], &ipfslog.LogOptions{ID: "A"})
			require.NoError(t, err)

			_, err = log1.Append(ctx, []byte("helloA1"), nil)
			require.NoError(t, err)
			_, err = log1.Append(ctx, []byte("helloA2"), nil)
			require.NoError(t, err)
			lastEntry1 := getLastEntry(log1.Values())

			_, err = log2.Append(ctx, []byte("helloB1"), nil)
			require.NoError(t, err)
			_, err = log2.Append(ctx, []byte("helloB2"), nil)
			require.NoError(t, err)
			lastEntry2 := getLastEntry(log2.Values())

			_, err = log1.Join(log2, -1)
			require.NoError(t, err)

			require.Equal(t, len(entry.FindHeads(log1.Entries)), 2)
			require.Equal(t, entry.FindHeads(log1.Entries)[0].GetHash().String(), lastEntry1.GetHash().String())
			require.Equal(t, entry.FindHeads(log1.Entries)[1].GetHash().String(), lastEntry2.GetHash().String())
		})

		t.Run("finds two heads after two joins", func(t *testing.T) {
			log1, err := ipfslog.NewLog(ipfs, identities[0], &ipfslog.LogOptions{ID: "A"})
			require.NoError(t, err)
			log2, err := ipfslog.NewLog(ipfs, identities[0], &ipfslog.LogOptions{ID: "A"})
			require.NoError(t, err)

			_, err = log1.Append(ctx, []byte("helloA1"), nil)
			require.NoError(t, err)
			_, err = log1.Append(ctx, []byte("helloA2"), nil)
			require.NoError(t, err)

			_, err = log2.Append(ctx, []byte("helloB1"), nil)
			require.NoError(t, err)
			_, err = log2.Append(ctx, []byte("helloB2"), nil)
			require.NoError(t, err)

			_, err = log1.Join(log2, -1)
			require.NoError(t, err)

			_, err = log2.Append(ctx, []byte("helloB3"), nil)
			require.NoError(t, err)

			_, err = log1.Append(ctx, []byte("helloA3"), nil)
			require.NoError(t, err)
			_, err = log1.Append(ctx, []byte("helloA4"), nil)
			require.NoError(t, err)

			lastEntry1 := getLastEntry(log1.Values())
			lastEntry2 := getLastEntry(log2.Values())

			_, err = log1.Join(log2, -1)
			require.NoError(t, err)

			require.Equal(t, len(entry.FindHeads(log1.Entries)), 2)
			require.Equal(t, entry.FindHeads(log1.Entries)[0].GetHash().String(), lastEntry1.GetHash().String())
			require.Equal(t, entry.FindHeads(log1.Entries)[1].GetHash().String(), lastEntry2.GetHash().String())
		})

		t.Run("finds two heads after three joins", func(t *testing.T) {
			log1, err := ipfslog.NewLog(ipfs, identities[0], &ipfslog.LogOptions{ID: "A"})
			require.NoError(t, err)
			log2, err := ipfslog.NewLog(ipfs, identities[0], &ipfslog.LogOptions{ID: "A"})
			require.NoError(t, err)
			log3, err := ipfslog.NewLog(ipfs, identities[0], &ipfslog.LogOptions{ID: "A"})
			require.NoError(t, err)

			_, err = log1.Append(ctx, []byte("helloA1"), nil)
			require.NoError(t, err)
			_, err = log1.Append(ctx, []byte("helloA2"), nil)
			require.NoError(t, err)
			_, err = log2.Append(ctx, []byte("helloB1"), nil)
			require.NoError(t, err)
			_, err = log2.Append(ctx, []byte("helloB2"), nil)
			require.NoError(t, err)
			_, err = log1.Join(log2, -1)
			require.NoError(t, err)
			_, err = log1.Append(ctx, []byte("helloA3"), nil)
			require.NoError(t, err)
			_, err = log1.Append(ctx, []byte("helloA4"), nil)
			require.NoError(t, err)
			lastEntry1 := getLastEntry(log1.Values())
			_, err = log3.Append(ctx, []byte("helloC1"), nil)
			require.NoError(t, err)
			_, err = log3.Append(ctx, []byte("helloC2"), nil)
			require.NoError(t, err)
			_, err = log2.Join(log3, -1)
			require.NoError(t, err)
			_, err = log2.Append(ctx, []byte("helloB3"), nil)
			require.NoError(t, err)
			lastEntry2 := getLastEntry(log2.Values())
			_, err = log1.Join(log2, -1)
			require.NoError(t, err)

			require.Equal(t, len(entry.FindHeads(log1.Entries)), 2)
			require.Equal(t, entry.FindHeads(log1.Entries)[0].GetHash().String(), lastEntry1.GetHash().String())
			require.Equal(t, entry.FindHeads(log1.Entries)[1].GetHash().String(), lastEntry2.GetHash().String())
		})

		t.Run("finds three heads after three joins", func(t *testing.T) {
			log1, err := ipfslog.NewLog(ipfs, identities[0], &ipfslog.LogOptions{ID: "A"})
			require.NoError(t, err)
			log2, err := ipfslog.NewLog(ipfs, identities[0], &ipfslog.LogOptions{ID: "A"})
			require.NoError(t, err)
			log3, err := ipfslog.NewLog(ipfs, identities[0], &ipfslog.LogOptions{ID: "A"})
			require.NoError(t, err)

			_, err = log1.Append(ctx, []byte("helloA1"), nil)
			require.NoError(t, err)
			_, err = log1.Append(ctx, []byte("helloA2"), nil)
			require.NoError(t, err)
			_, err = log2.Append(ctx, []byte("helloB1"), nil)
			require.NoError(t, err)
			_, err = log2.Append(ctx, []byte("helloB2"), nil)
			require.NoError(t, err)
			_, err = log1.Join(log2, -1)
			require.NoError(t, err)
			_, err = log1.Append(ctx, []byte("helloA3"), nil)
			require.NoError(t, err)
			_, err = log1.Append(ctx, []byte("helloA4"), nil)
			require.NoError(t, err)
			lastEntry1 := getLastEntry(log1.Values())
			_, err = log3.Append(ctx, []byte("helloC1"), nil)
			require.NoError(t, err)
			_, err = log2.Append(ctx, []byte("helloB3"), nil)
			require.NoError(t, err)
			_, err = log3.Append(ctx, []byte("helloC2"), nil)
			require.NoError(t, err)
			lastEntry2 := getLastEntry(log2.Values())
			lastEntry3 := getLastEntry(log3.Values())
			_, err = log1.Join(log2, -1)
			require.NoError(t, err)
			_, err = log1.Join(log3, -1)
			require.NoError(t, err)

			require.Equal(t, len(entry.FindHeads(log1.Entries)), 3)
			require.Equal(t, entry.FindHeads(log1.Entries)[0].GetHash().String(), lastEntry1.GetHash().String())
			require.Equal(t, entry.FindHeads(log1.Entries)[1].GetHash().String(), lastEntry2.GetHash().String())
			require.Equal(t, entry.FindHeads(log1.Entries)[2].GetHash().String(), lastEntry3.GetHash().String())
		})
	})

	t.Run("tails", func(t *testing.T) {
		// TODO: implements findTails(orderedmap)
		// t.Run("returns a tail", func(t *testing.T) {
		// 	log1, err := log.NewLog(ipfs, identities[0], &log.LogOptions{ID: "A"})
		// 	require.NoError(t, err)
		// 	_, err = log1.Append([]byte("helloA1"), nil)
		// 	require.NoError(t, err)
		//      require.Equal(t, len(log.FindTails(log1.Entries)), 1)
		// })

		// t.Run("returns tail entries", func(t *testing.T) {
		// 	log1, err := log.NewLog(ipfs, identities[0], &log.LogOptions{ID: "A"})
		// 	require.NoError(t, err)
		// 	log2, err := log.NewLog(ipfs, identities[0], &log.LogOptions{ID: "A"})
		// 	require.NoError(t, err)
		// 	_, err = log1.Append([]byte("helloA1"), nil)
		// 	require.NoError(t, err)
		// 	_, err = log1.Append([]byte("helloA1"), nil)
		// 	require.NoError(t, err)
		//      require.Equal(t, len(log.FindTails(log1.Entries)), 1)
		// })
	})
}
