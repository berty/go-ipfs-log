package test

import (
	"context"
	"fmt"
	"testing"

	dssync "github.com/ipfs/go-datastore/sync"
	mocknet "github.com/libp2p/go-libp2p/p2p/net/mock"

	ipfslog "berty.tech/go-ipfs-log"
	idp "berty.tech/go-ipfs-log/identityprovider"
	"berty.tech/go-ipfs-log/keystore"
)

func BenchmarkAdd(b *testing.B) {
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

	identity, err := idp.CreateIdentity(&idp.CreateIdentityOptions{
		Keystore: ks,
		ID:       "userA",
		Type:     "orbitdb",
	})

	log, err := ipfslog.NewLog(ipfs, identity, &ipfslog.LogOptions{ID: "A"})
	if err != nil {
		b.Fatal(err)
	}

	b.ResetTimer()
	// Start the main loop
	for n := 0; n < b.N; n++ {
		if _, err := log.Append(ctx, []byte(fmt.Sprintf("%d", n)), nil); err != nil {
			b.Fatal(err)
		}
	}
}
