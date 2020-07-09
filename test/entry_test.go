package test

import (
	"context"
	"fmt"
	"testing"

	ipfslog "berty.tech/go-ipfs-log"
	"berty.tech/go-ipfs-log/entry"
	"berty.tech/go-ipfs-log/errmsg"
	idp "berty.tech/go-ipfs-log/identityprovider"
	"berty.tech/go-ipfs-log/io"
	ks "berty.tech/go-ipfs-log/keystore"
	cid "github.com/ipfs/go-cid"
	dssync "github.com/ipfs/go-datastore/sync"
	mocknet "github.com/libp2p/go-libp2p/p2p/net/mock"
	"github.com/stretchr/testify/require"
)

func TestEntry(t *testing.T) {
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
		ID:       fmt.Sprintf("userA"),
		Type:     "orbitdb",
	})
	require.NoError(t, err)

	t.Run("create", func(t *testing.T) {
		t.Run("creates an empty entry", func(t *testing.T) {
			expectedHash := CidB32(t, "zdpuAsPdzSyeux5mFsFV1y3WeHAShGNi4xo22cYBYWUdPtxVB")

			e, err := entry.CreateEntry(ctx, ipfs, identity, &entry.Entry{Payload: []byte("hello"), LogID: "A"}, nil)
			require.NoError(t, err)
			require.NotNil(t, e)

			require.Equal(t, e.Hash.String(), expectedHash)
			require.Equal(t, e.LogID, "A")
			require.Equal(t, e.Clock.GetID(), identity.PublicKey)
			require.Equal(t, e.Clock.GetTime(), 0)
			require.Equal(t, e.V, uint64(2))
			require.Equal(t, string(e.Payload), "hello")
			require.Equal(t, len(e.Next), 0)
			require.Equal(t, len(e.Refs), 0)
		})

		t.Run("creates an entry with payload", func(t *testing.T) {
			expectedHash := CidB32(t, "zdpuAyvJU3TS7LUdfRxwAnJorkz6NfpAWHGypsQEXLZxcCCRC")
			e, err := entry.CreateEntry(ctx, ipfs, identity, &entry.Entry{Payload: []byte("hello world"), LogID: "A"}, nil)
			require.NoError(t, err)
			require.NotNil(t, e)

			require.Equal(t, string(e.Payload), "hello world")
			require.Equal(t, e.LogID, "A")
			require.Equal(t, e.Clock.GetID(), identity.PublicKey)
			require.Equal(t, e.Clock.GetTime(), 0)
			require.Equal(t, e.V, uint64(2))
			require.Equal(t, len(e.Next), 0)
			require.Equal(t, len(e.Refs), 0)
			require.Equal(t, e.Hash.String(), expectedHash)
		})

		t.Run("creates an entry with payload and next", func(t *testing.T) {
			expectedHash := CidB32(t, "zdpuAqsN9Py4EWSfrGYZS8tuokWuiTd9zhS8dhr9XpSGQajP2")
			payload1 := "hello world"
			payload2 := "hello again"
			e1, err := entry.CreateEntry(ctx, ipfs, identity, &entry.Entry{Payload: []byte(payload1), LogID: "A"}, nil)
			require.NoError(t, err)
			e1.Clock.Tick()
			e2, err := entry.CreateEntry(ctx, ipfs, identity, &entry.Entry{Payload: []byte(payload2), LogID: "A", Next: []cid.Cid{e1.Hash}, Clock: e1.Clock}, nil)
			require.NoError(t, err)

			require.Equal(t, string(e2.Payload), payload2)
			require.Equal(t, len(e2.Next), 1)
			require.Equal(t, e2.Hash.String(), expectedHash)
			require.Equal(t, e2.Clock.GetID(), identity.PublicKey)
			require.Equal(t, e2.Clock.GetTime(), 1)
		})

		t.Run("should return an entry interopable with older versions", func(t *testing.T) {
			expectedHashV1 := CidB32(t, "zdpuAsPdzSyeux5mFsFV1y3WeHAShGNi4xo22cYBYWUdPtxVB")
			entryV1, err := entry.CreateEntry(ctx, ipfs, identity, &entry.Entry{LogID: "A", Payload: []byte("hello")}, nil)
			require.NoError(t, err)

			logV1, err := ipfslog.NewFromEntryHash(ctx, ipfs, identity, entryV1.GetHash(), &ipfslog.LogOptions{ID: "A"}, &ipfslog.FetchOptions{})
			require.NoError(t, err)

			require.Equal(t, entryV1.GetHash().String(), expectedHashV1)

			id, err := cid.Parse(expectedHashV1)
			require.NoError(t, err)

			e, ok := logV1.Get(id)
			require.True(t, ok)
			require.Equal(t, e.GetHash().String(), expectedHashV1)
		})

		t.Run("returns an error if ipfs is not set", func(t *testing.T) {
			e, err := entry.CreateEntry(ctx, nil, identity, &entry.Entry{Payload: []byte("hello"), LogID: "A"}, nil)
			require.Nil(t, e)
			require.Error(t, err)
			require.Equal(t, err, errmsg.ErrIPFSNotDefined)
		})

		t.Run("returns an error if identity is not set", func(t *testing.T) {
			e, err := entry.CreateEntry(ctx, ipfs, nil, &entry.Entry{Payload: []byte("hello"), LogID: "A"}, nil)
			require.Nil(t, e)
			require.Error(t, err)
			require.Equal(t, err, errmsg.ErrIdentityNotDefined)
		})

		t.Run("returns an error if data is not set", func(t *testing.T) {
			e, err := entry.CreateEntry(ctx, ipfs, identity, nil, nil)
			require.Nil(t, e)
			require.Error(t, err)
			require.Equal(t, err, errmsg.ErrPayloadNotDefined)
		})

		t.Run("returns an error if LogID is not set", func(t *testing.T) {
			e, err := entry.CreateEntry(ctx, ipfs, identity, &entry.Entry{Payload: []byte("hello")}, nil)
			require.Nil(t, e)
			require.Error(t, err)
			require.Equal(t, err, errmsg.ErrLogIDNotDefined)
		})
	})

	t.Run("toMultihash", func(t *testing.T) {
		t.Run("returns an ipfs multihash", func(t *testing.T) {
			expectedHash := CidB32(t, "zdpuAsPdzSyeux5mFsFV1y3WeHAShGNi4xo22cYBYWUdPtxVB")
			e, err := entry.CreateEntry(ctx, ipfs, identity, &entry.Entry{Payload: []byte("hello"), LogID: "A"}, nil)
			require.NoError(t, err)

			hash, err := e.ToMultihash(ctx, ipfs, nil)
			require.NoError(t, err)

			require.Equal(t, e.Hash.String(), expectedHash)
			require.Equal(t, hash.String(), expectedHash)
		})

		t.Run("returns the correct ipfs multihash for a v1 entry", func(t *testing.T) {
			e := getEntriesV1Fixtures(t, identity)[0]
			expectedHash := CidB32(t, "zdpuAsJDrLKrAiU8M518eu6mgv9HzS3e1pfH5XC7LUsFgsK5c")

			hash, err := e.ToMultihash(ctx, ipfs, nil)
			require.NoError(t, err)

			require.Equal(t, hash.String(), expectedHash)
		})

		// TODO
		// t.Run("returns the correct ipfs hash (multihash) for a v0 entry", func(t *testing.T) {
		// 		expectedHash := "QmV5NpvViHHouBfo7CSnfX2iB4t5PVWNJG8doKt5cwwnxY"
		// 		_ = expectedHash
		// 	})
	})

	// TODO
	t.Run("fromMultihash", func(t *testing.T) {
		t.Run("creates a entry from ipfs hash", func(t *testing.T) {
			expectedHash := CidB32(t, "zdpuAnRGWKPkMHqumqdkRJtzbyW6qAGEiBRv61Zj3Ts4j9tQF")

			payload1 := []byte("hello world")
			payload2 := []byte("hello again")
			entry1, err := entry.CreateEntry(ctx, ipfs, identity, &entry.Entry{Payload: payload1, LogID: "A"}, nil)
			require.NoError(t, err)

			entry2, err := entry.CreateEntry(ctx, ipfs, identity, &entry.Entry{Payload: payload2, LogID: "A", Next: []cid.Cid{entry1.Hash}}, nil)
			require.NoError(t, err)

			final, err := entry.FromMultihash(ctx, ipfs, entry2.Hash, identity.Provider)
			require.NoError(t, err)

			require.Equal(t, final.LogID, "A")
			require.Equal(t, final.Payload, payload2)
			require.Equal(t, len(final.Next), 1)
			require.Equal(t, final.Hash.String(), expectedHash)
		})

		t.Run("creates a entry from ipfs multihash of v1 entries", func(t *testing.T) {
			expectedHash := CidB32(t, "zdpuAxgKyiM9qkP9yPKCCqrHer9kCqYyr7KbhucsPwwfh6JB3")
			e1 := getEntriesV1Fixtures(t, identity)[0]
			e2 := getEntriesV1Fixtures(t, identity)[1]

			entry1Hash, err := io.WriteCBOR(ctx, ipfs, e1.ToCborEntry(), nil)
			require.NoError(t, err)

			entry2Hash, err := io.WriteCBOR(ctx, ipfs, e2.ToCborEntry(), nil)
			require.NoError(t, err)

			final, err := entry.FromMultihash(ctx, ipfs, entry2Hash, identity.Provider)
			require.NoError(t, err)

			require.Equal(t, final.LogID, "A")
			require.Equal(t, final.Payload, e2.Payload)
			require.Equal(t, len(final.Next), 1)
			require.Equal(t, final.Next[0].String(), e2.Next[0].String())
			require.Equal(t, final.Next[0].String(), entry1Hash.String())
			require.Equal(t, final.V, uint64(1))
			require.Equal(t, final.Hash.String(), entry2Hash.String())
			require.Equal(t, entry2Hash.String(), expectedHash)
		})
	})

	t.Run("isParent", func(t *testing.T) {
		t.Run("returns true if entry has a child", func(t *testing.T) {
			payload1 := "hello world"
			payload2 := "hello again"
			e1, err := entry.CreateEntry(ctx, ipfs, identity, &entry.Entry{Payload: []byte(payload1), LogID: "A"}, nil)
			require.NoError(t, err)

			e2, err := entry.CreateEntry(ctx, ipfs, identity, &entry.Entry{Payload: []byte(payload2), LogID: "A", Next: []cid.Cid{e1.Hash}}, nil)
			require.NoError(t, err)
			require.True(t, e1.IsParent(e2))
		})

		t.Run("returns false if entry has a child", func(t *testing.T) {
			payload1 := "hello world"
			payload2 := "hello again"
			e1, err := entry.CreateEntry(ctx, ipfs, identity, &entry.Entry{Payload: []byte(payload1), LogID: "A"}, nil)
			require.NoError(t, err)
			e2, err := entry.CreateEntry(ctx, ipfs, identity, &entry.Entry{Payload: []byte(payload2), LogID: "A"}, nil)
			require.NoError(t, err)
			e3, err := entry.CreateEntry(ctx, ipfs, identity, &entry.Entry{Payload: []byte(payload2), LogID: "A", Next: []cid.Cid{e2.Hash}}, nil)
			require.NoError(t, err)

			require.False(t, e1.IsParent(e2))
			require.False(t, e1.IsParent(e3))
			require.True(t, e2.IsParent(e3))
		})
	})

	t.Run("compare", func(t *testing.T) {
		t.Run("returns true if entries are the same", func(t *testing.T) {
			payload1 := "hello world"
			e1, err := entry.CreateEntry(ctx, ipfs, identity, &entry.Entry{Payload: []byte(payload1), LogID: "A"}, nil)
			require.NoError(t, err)

			e2, err := entry.CreateEntry(ctx, ipfs, identity, &entry.Entry{Payload: []byte(payload1), LogID: "A"}, nil)
			require.NoError(t, err)
			require.True(t, e1.Equals(e2))
		})

		t.Run("returns true if entries are not the same", func(t *testing.T) {
			payload1 := "hello world"
			payload2 := "hello again"
			e1, err := entry.CreateEntry(ctx, ipfs, identity, &entry.Entry{Payload: []byte(payload1), LogID: "A"}, nil)
			require.NoError(t, err)

			e2, err := entry.CreateEntry(ctx, ipfs, identity, &entry.Entry{Payload: []byte(payload2), LogID: "A"}, nil)
			require.NoError(t, err)
			require.False(t, e1.Equals(e2))
		})
	})

	// TODO
	// t.Run("isEntry", func(t *testing.T) {
	// })
}
