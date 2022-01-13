package test

import (
	"context"
	"fmt"
	"testing"

	ipfslog "berty.tech/go-ipfs-log"
	idp "berty.tech/go-ipfs-log/identityprovider"
	ks "berty.tech/go-ipfs-log/keystore"
	dssync "github.com/ipfs/go-datastore/sync"
	mocknet "github.com/libp2p/go-libp2p/p2p/net/mock"
	"github.com/stretchr/testify/require"
)

func TestLogCRDT(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	m := mocknet.New(ctx)
	ipfs, closeNode := NewMemoryServices(ctx, t, m)
	defer closeNode()

	datastore := dssync.MutexWrap(NewIdentityDataStore(t))
	keystore, err := ks.NewKeystore(datastore)
	require.NoError(t, err)

	var identities [3]*idp.Identity

	for i, char := range []rune{'A', 'B', 'C'} {
		identity, err := idp.CreateIdentity(ctx, &idp.CreateIdentityOptions{
			Keystore: keystore,
			ID:       fmt.Sprintf("user%c", char),
			Type:     "orbitdb",
		})
		require.NoError(t, err)

		identities[i] = identity
	}

	// setup
	var log1, log2, log3 *ipfslog.IPFSLog
	setup := func(t *testing.T) {
		t.Helper()

		var err error
		log1, err = ipfslog.NewLog(ipfs, identities[0], &ipfslog.LogOptions{ID: "X"})
		require.NoError(t, err)

		log2, err = ipfslog.NewLog(ipfs, identities[1], &ipfslog.LogOptions{ID: "X"})
		require.NoError(t, err)

		log3, err = ipfslog.NewLog(ipfs, identities[2], &ipfslog.LogOptions{ID: "X"})
		require.NoError(t, err)
	}

	t.Run("join is associative", func(t *testing.T) {
		setup(t)
		const expectedElementsCount = 6

		_, err = log1.Append(ctx, []byte("helloA1"), nil)
		require.NoError(t, err)
		_, err = log1.Append(ctx, []byte("helloA2"), nil)
		require.NoError(t, err)
		_, err = log2.Append(ctx, []byte("helloB1"), nil)
		require.NoError(t, err)
		_, err = log2.Append(ctx, []byte("helloB2"), nil)
		require.NoError(t, err)
		_, err = log3.Append(ctx, []byte("helloC1"), nil)
		require.NoError(t, err)
		_, err = log3.Append(ctx, []byte("helloC2"), nil)
		require.NoError(t, err)

		// a + (b + c)
		_, err = log2.Join(log3, -1)
		require.NoError(t, err)

		_, err = log1.Join(log2, -1)
		require.NoError(t, err)

		res1 := log1.ToString(nil)
		res1Len := log1.Values().Len()

		log1, err = ipfslog.NewLog(ipfs, identities[0], &ipfslog.LogOptions{ID: "X"})
		require.NoError(t, err)

		log2, err = ipfslog.NewLog(ipfs, identities[1], &ipfslog.LogOptions{ID: "X"})
		require.NoError(t, err)

		log3, err = ipfslog.NewLog(ipfs, identities[2], &ipfslog.LogOptions{ID: "X"})
		require.NoError(t, err)

		_, err = log1.Append(ctx, []byte("helloA1"), nil)
		require.NoError(t, err)

		_, err = log1.Append(ctx, []byte("helloA2"), nil)
		require.NoError(t, err)

		_, err = log2.Append(ctx, []byte("helloB1"), nil)
		require.NoError(t, err)

		_, err = log2.Append(ctx, []byte("helloB2"), nil)
		require.NoError(t, err)

		_, err = log3.Append(ctx, []byte("helloC1"), nil)
		require.NoError(t, err)

		_, err = log3.Append(ctx, []byte("helloC2"), nil)
		require.NoError(t, err)

		// (a + b) + c
		_, err = log1.Join(log2, -1)
		require.NoError(t, err)

		_, err = log3.Join(log1, -1)
		require.NoError(t, err)

		res2 := log3.ToString(nil)
		res2Len := log3.Values().Len()

		// associativity: a + (b + c) == (a + b) + c

		require.Equal(t, res1Len, expectedElementsCount)
		require.Equal(t, res2Len, expectedElementsCount)
		require.Equal(t, res1, res2)
	})

	t.Run("join is commutative", func(t *testing.T) {
		setup(t)
		const expectedElementsCount = 4

		_, err = log1.Append(ctx, []byte("helloA1"), nil)
		require.NoError(t, err)

		_, err = log1.Append(ctx, []byte("helloA2"), nil)
		require.NoError(t, err)

		_, err = log2.Append(ctx, []byte("helloB1"), nil)
		require.NoError(t, err)

		_, err = log2.Append(ctx, []byte("helloB2"), nil)
		require.NoError(t, err)

		// b + a
		_, err = log2.Join(log1, -1)
		require.NoError(t, err)

		res1 := log2.ToString(nil)
		res1Len := log2.Values().Len()

		log1, err = ipfslog.NewLog(ipfs, identities[0], &ipfslog.LogOptions{ID: "X"})
		require.NoError(t, err)

		log2, err = ipfslog.NewLog(ipfs, identities[1], &ipfslog.LogOptions{ID: "X"})
		require.NoError(t, err)

		_, err = log1.Append(ctx, []byte("helloA1"), nil)
		require.NoError(t, err)

		_, err = log1.Append(ctx, []byte("helloA2"), nil)
		require.NoError(t, err)

		_, err = log2.Append(ctx, []byte("helloB1"), nil)
		require.NoError(t, err)

		_, err = log2.Append(ctx, []byte("helloB2"), nil)
		require.NoError(t, err)

		// a + b
		_, err = log1.Join(log2, -1)
		require.NoError(t, err)

		res2 := log1.ToString(nil)
		res2Len := log1.Values().Len()

		// commutativity: a + b == b + a
		require.Equal(t, res1Len, expectedElementsCount)
		require.Equal(t, res2Len, expectedElementsCount)
		require.Equal(t, res1, res2)
	})

	t.Run("multiple joins are commutative", func(t *testing.T) {
		setup(t)

		// b + a == a + b
		log1, err := ipfslog.NewLog(ipfs, identities[0], &ipfslog.LogOptions{ID: "X"})
		require.NoError(t, err)

		log2, err := ipfslog.NewLog(ipfs, identities[1], &ipfslog.LogOptions{ID: "X"})
		require.NoError(t, err)

		_, err = log1.Append(ctx, []byte("helloA1"), nil)
		require.NoError(t, err)

		_, err = log1.Append(ctx, []byte("helloA2"), nil)
		require.NoError(t, err)

		_, err = log2.Append(ctx, []byte("helloB1"), nil)
		require.NoError(t, err)

		_, err = log2.Append(ctx, []byte("helloB2"), nil)
		require.NoError(t, err)

		_, err = log2.Join(log1, -1)

		resA1 := log2.ToString(nil)

		log1, err = ipfslog.NewLog(ipfs, identities[0], &ipfslog.LogOptions{ID: "X"})
		require.NoError(t, err)

		log2, err = ipfslog.NewLog(ipfs, identities[1], &ipfslog.LogOptions{ID: "X"})
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

		resA2 := log1.ToString(nil)

		require.Equal(t, resA1, resA2)

		// a + b == b + a
		log1, err = ipfslog.NewLog(ipfs, identities[0], &ipfslog.LogOptions{ID: "X"})
		require.NoError(t, err)

		log2, err = ipfslog.NewLog(ipfs, identities[1], &ipfslog.LogOptions{ID: "X"})
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

		resB1 := log1.ToString(nil)

		log1, err = ipfslog.NewLog(ipfs, identities[0], &ipfslog.LogOptions{ID: "X"})
		require.NoError(t, err)

		log2, err = ipfslog.NewLog(ipfs, identities[1], &ipfslog.LogOptions{ID: "X"})
		require.NoError(t, err)

		_, err = log1.Append(ctx, []byte("helloA1"), nil)
		require.NoError(t, err)

		_, err = log1.Append(ctx, []byte("helloA2"), nil)
		require.NoError(t, err)

		_, err = log2.Append(ctx, []byte("helloB1"), nil)
		require.NoError(t, err)

		_, err = log2.Append(ctx, []byte("helloB2"), nil)
		require.NoError(t, err)

		_, err = log2.Join(log1, -1)
		require.NoError(t, err)

		resB2 := log2.ToString(nil)

		require.Equal(t, resB1, resB2)

		// a + c == c + a
		log1, err = ipfslog.NewLog(ipfs, identities[0], &ipfslog.LogOptions{ID: "X"})
		require.NoError(t, err)

		log3, err = ipfslog.NewLog(ipfs, identities[2], &ipfslog.LogOptions{ID: "X"})
		require.NoError(t, err)

		_, err = log1.Append(ctx, []byte("helloA1"), nil)
		require.NoError(t, err)

		_, err = log1.Append(ctx, []byte("helloA2"), nil)
		require.NoError(t, err)

		_, err = log3.Append(ctx, []byte("helloC1"), nil)
		require.NoError(t, err)

		_, err = log3.Append(ctx, []byte("helloC2"), nil)
		require.NoError(t, err)

		_, err = log3.Join(log1, -1)
		require.NoError(t, err)

		resC1 := log3.ToString(nil)

		log1, err = ipfslog.NewLog(ipfs, identities[0], &ipfslog.LogOptions{ID: "X"})
		require.NoError(t, err)

		log3, err = ipfslog.NewLog(ipfs, identities[2], &ipfslog.LogOptions{ID: "X"})
		require.NoError(t, err)

		_, err = log1.Append(ctx, []byte("helloA1"), nil)
		require.NoError(t, err)

		_, err = log1.Append(ctx, []byte("helloA2"), nil)
		require.NoError(t, err)

		_, err = log3.Append(ctx, []byte("helloC1"), nil)
		require.NoError(t, err)

		_, err = log3.Append(ctx, []byte("helloC2"), nil)
		require.NoError(t, err)

		_, err = log1.Join(log3, -1)

		resC2 := log1.ToString(nil)

		require.Equal(t, resC1, resC2)

		// c + b == b + c
		log2, err = ipfslog.NewLog(ipfs, identities[1], &ipfslog.LogOptions{ID: "X"})
		require.NoError(t, err)

		log3, err = ipfslog.NewLog(ipfs, identities[2], &ipfslog.LogOptions{ID: "X"})
		require.NoError(t, err)

		_, err = log2.Append(ctx, []byte("helloB1"), nil)
		require.NoError(t, err)

		_, err = log2.Append(ctx, []byte("helloB2"), nil)
		require.NoError(t, err)

		_, err = log3.Append(ctx, []byte("helloC1"), nil)
		require.NoError(t, err)

		_, err = log3.Append(ctx, []byte("helloC2"), nil)
		require.NoError(t, err)

		_, err = log3.Join(log2, -1)
		require.NoError(t, err)

		resD1 := log3.ToString(nil)

		log2, err = ipfslog.NewLog(ipfs, identities[1], &ipfslog.LogOptions{ID: "X"})
		require.NoError(t, err)

		log3, err = ipfslog.NewLog(ipfs, identities[2], &ipfslog.LogOptions{ID: "X"})
		require.NoError(t, err)

		_, err = log2.Append(ctx, []byte("helloB1"), nil)
		require.NoError(t, err)

		_, err = log2.Append(ctx, []byte("helloB2"), nil)
		require.NoError(t, err)

		_, err = log3.Append(ctx, []byte("helloC1"), nil)
		require.NoError(t, err)

		_, err = log3.Append(ctx, []byte("helloC2"), nil)
		require.NoError(t, err)

		_, err = log2.Join(log3, -1)
		require.NoError(t, err)

		resD2 := log2.ToString(nil)

		require.Equal(t, resD1, resD2)

		// a + b + c == c + b + a
		log1, err = ipfslog.NewLog(ipfs, identities[0], &ipfslog.LogOptions{ID: "X"})
		require.NoError(t, err)

		log2, err = ipfslog.NewLog(ipfs, identities[1], &ipfslog.LogOptions{ID: "X"})
		require.NoError(t, err)

		log3, err = ipfslog.NewLog(ipfs, identities[2], &ipfslog.LogOptions{ID: "X"})
		require.NoError(t, err)

		_, err = log1.Append(ctx, []byte("helloA1"), nil)
		require.NoError(t, err)

		_, err = log1.Append(ctx, []byte("helloA2"), nil)
		require.NoError(t, err)

		_, err = log2.Append(ctx, []byte("helloB1"), nil)
		require.NoError(t, err)

		_, err = log2.Append(ctx, []byte("helloB2"), nil)
		require.NoError(t, err)

		_, err = log3.Append(ctx, []byte("helloC1"), nil)
		require.NoError(t, err)

		_, err = log3.Append(ctx, []byte("helloC2"), nil)
		require.NoError(t, err)

		_, err = log1.Join(log2, -1)
		require.NoError(t, err)

		_, err = log1.Join(log3, -1)
		require.NoError(t, err)

		logLeft := log1.ToString(nil)

		log1, err = ipfslog.NewLog(ipfs, identities[0], &ipfslog.LogOptions{ID: "X"})
		require.NoError(t, err)

		log2, err = ipfslog.NewLog(ipfs, identities[1], &ipfslog.LogOptions{ID: "X"})
		require.NoError(t, err)

		log3, err = ipfslog.NewLog(ipfs, identities[2], &ipfslog.LogOptions{ID: "X"})
		require.NoError(t, err)

		_, err = log1.Append(ctx, []byte("helloA1"), nil)
		require.NoError(t, err)

		_, err = log1.Append(ctx, []byte("helloA2"), nil)
		require.NoError(t, err)

		_, err = log2.Append(ctx, []byte("helloB1"), nil)
		require.NoError(t, err)

		_, err = log2.Append(ctx, []byte("helloB2"), nil)
		require.NoError(t, err)

		_, err = log3.Append(ctx, []byte("helloC1"), nil)
		require.NoError(t, err)

		_, err = log3.Append(ctx, []byte("helloC2"), nil)
		require.NoError(t, err)

		_, err = log3.Join(log2, -1)
		require.NoError(t, err)

		_, err = log3.Join(log1, -1)
		require.NoError(t, err)

		logRight := log3.ToString(nil)

		require.Equal(t, logLeft, logRight)
	})

	t.Run("join is idempotent", func(t *testing.T) {
		setup(t)

		expectedElementsCount := 3

		logA, err := ipfslog.NewLog(ipfs, identities[0], &ipfslog.LogOptions{ID: "X"})
		require.NoError(t, err)

		_, err = logA.Append(ctx, []byte("helloA1"), nil)
		require.NoError(t, err)

		_, err = logA.Append(ctx, []byte("helloA2"), nil)
		require.NoError(t, err)

		_, err = logA.Append(ctx, []byte("helloA3"), nil)
		require.NoError(t, err)

		// idempotence: a + a = a
		_, err = logA.Join(logA, -1)
		require.Equal(t, logA.Entries.Len(), expectedElementsCount)
	})
}
