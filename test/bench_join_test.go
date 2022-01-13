package test

import (
	"context"
	"fmt"
	"sync"
	"testing"

	ipfslog "berty.tech/go-ipfs-log"
	idp "berty.tech/go-ipfs-log/identityprovider"
	"berty.tech/go-ipfs-log/keystore"
	dssync "github.com/ipfs/go-datastore/sync"
	mocknet "github.com/libp2p/go-libp2p/p2p/net/mock"
	"github.com/stretchr/testify/require"
)

func BenchmarkJoin(b *testing.B) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	m := mocknet.New(ctx)
	ipfs, closeNode := NewMemoryServices(ctx, b, m)
	defer closeNode()

	datastore := dssync.MutexWrap(NewIdentityDataStore(b))
	ks, err := keystore.NewKeystore(datastore)
	require.NoError(b, err)

	identityA, err := idp.CreateIdentity(ctx, &idp.CreateIdentityOptions{
		Keystore: ks,
		ID:       "userA",
		Type:     "orbitdb",
	})
	require.NoError(b, err)

	identityB, err := idp.CreateIdentity(ctx, &idp.CreateIdentityOptions{
		Keystore: ks,
		ID:       "userB",
		Type:     "orbitdb",
	})
	require.NoError(b, err)

	logA, err := ipfslog.NewLog(ipfs, identityA, &ipfslog.LogOptions{ID: "A"})
	require.NoError(b, err)

	logB, err := ipfslog.NewLog(ipfs, identityB, &ipfslog.LogOptions{ID: "A"})
	require.NoError(b, err)

	b.ResetTimer()
	// Start the main loop
	for n := 0; n < b.N; n++ {
		wg := sync.WaitGroup{}
		wg.Add(2)

		go func() {
			_, err := logA.Append(ctx, []byte(fmt.Sprintf("a%d", n)), nil)
			require.NoError(b, err)
			wg.Done()
		}()

		go func() {
			_, err := logB.Append(ctx, []byte(fmt.Sprintf("a%d", n)), nil)
			require.NoError(b, err)
			wg.Done()
		}()

		wg.Wait()

		_, err := logA.Join(logB, -1)
		require.NoError(b, err)

		_, err = logB.Join(logA, -1)
		require.NoError(b, err)
	}
}
