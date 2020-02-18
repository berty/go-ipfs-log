package test

import (
	"context"
	"fmt"
	"sync"
	"testing"

	dssync "github.com/ipfs/go-datastore/sync"
	mocknet "github.com/libp2p/go-libp2p/p2p/net/mock"

	ipfslog "berty.tech/go-ipfs-log"
	idp "berty.tech/go-ipfs-log/identityprovider"
	"berty.tech/go-ipfs-log/keystore"
)

func BenchmarkJoin(b *testing.B) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	m := mocknet.New(ctx)
	ipfs, closeNode := NewMemoryServices(ctx, b, m)
	defer closeNode()

	datastore := dssync.MutexWrap(NewIdentityDataStore(b))
	ks, err := keystore.NewKeystore(datastore)
	if err != nil {
		b.Fatal(err)
	}

	identityA, err := idp.CreateIdentity(&idp.CreateIdentityOptions{
		Keystore: ks,
		ID:       "userA",
		Type:     "orbitdb",
	})

	identityB, err := idp.CreateIdentity(&idp.CreateIdentityOptions{
		Keystore: ks,
		ID:       "userB",
		Type:     "orbitdb",
	})

	logA, err := ipfslog.NewLog(ipfs, identityA, &ipfslog.LogOptions{ID: "A"})
	if err != nil {
		b.Fatal(err)
	}

	logB, err := ipfslog.NewLog(ipfs, identityB, &ipfslog.LogOptions{ID: "A"})
	if err != nil {
		b.Fatal(err)
	}

	b.ResetTimer()
	// Start the main loop
	for n := 0; n < b.N; n++ {
		wg := sync.WaitGroup{}
		wg.Add(2)

		go func() {
			if _, err := logA.Append(ctx, []byte(fmt.Sprintf("a%d", n)), nil); err != nil {
				b.Fatal(err)
			}

			wg.Done()
		}()

		go func() {
			if _, err := logB.Append(ctx, []byte(fmt.Sprintf("a%d", n)), nil); err != nil {
				b.Fatal(err)
			}

			wg.Done()
		}()

		wg.Wait()

		if _, err := logA.Join(logB, -1); err != nil {
			b.Fatal(err)
		}

		if _, err := logB.Join(logA, -1); err != nil {
			b.Fatal(err)
		}
	}
}
