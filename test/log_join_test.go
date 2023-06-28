package test

import (
	"context"
	"fmt"
	"reflect"
	"testing"

	ipfslog "berty.tech/go-ipfs-log"
	"berty.tech/go-ipfs-log/entry"
	"berty.tech/go-ipfs-log/errmsg"
	idp "berty.tech/go-ipfs-log/identityprovider"
	"berty.tech/go-ipfs-log/iface"
	ks "berty.tech/go-ipfs-log/keystore"
	cid "github.com/ipfs/go-cid"
	dssync "github.com/ipfs/go-datastore/sync"
	mocknet "github.com/libp2p/go-libp2p/p2p/net/mock"
	"github.com/stretchr/testify/require"
)

func TestLogJoin(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	m := mocknet.New()
	defer m.Close()

	p, err := m.GenPeer()
	require.NoError(t, err)

	dag := setupDAGService(t, p)

	datastore := dssync.MutexWrap(NewIdentityDataStore(t))
	keystore, err := ks.NewKeystore(datastore)
	require.NoError(t, err)

	var identities [4]*idp.Identity

	for i, char := range []rune{'C', 'B', 'D', 'A'} {

		identity, err := idp.CreateIdentity(ctx, &idp.CreateIdentityOptions{
			Keystore: keystore,
			ID:       fmt.Sprintf("user%c", char),
			Type:     "orbitdb",
		})
		require.NoError(t, err)

		identities[i] = identity
	}

	// setup
	var logs []*ipfslog.IPFSLog
	setup := func(t *testing.T) {
		logs = []*ipfslog.IPFSLog{}
		for i := 0; i < 4; i++ {
			l, err := ipfslog.NewLog(dag, identities[i], &ipfslog.LogOptions{ID: "X"})
			require.NoError(t, err)

			logs = append(logs, l)
		}
	}

	t.Run("joins logs", func(t *testing.T) {
		setup(t)
		var items [3][]iface.IPFSLogEntry
		var prev [3]iface.IPFSLogEntry
		var curr [3]iface.IPFSLogEntry
		var err error

		curr[0], err = entry.CreateEntry(ctx, dag, identities[0], &entry.Entry{Payload: []byte("entryA1"), LogID: "X"}, nil)
		require.NoError(t, err)

		curr[1], err = entry.CreateEntry(ctx, dag, identities[1], &entry.Entry{Payload: []byte("entryB1"), LogID: "X", Next: []cid.Cid{curr[0].GetHash()}}, nil)
		require.NoError(t, err)

		curr[2], err = entry.CreateEntry(ctx, dag, identities[2], &entry.Entry{Payload: []byte("entryC1"), LogID: "X", Next: []cid.Cid{curr[0].GetHash(), curr[1].GetHash()}}, nil)
		require.NoError(t, err)

		for i := 1; i <= 100; i++ {
			if i > 1 {
				for j := 0; j < 3; j++ {
					prev[j] = items[j][len(items[j])-1]
				}
				curr[0], err = entry.CreateEntry(ctx, dag, identities[0], &entry.Entry{Payload: []byte(fmt.Sprintf("entryA%d", i)), LogID: "X", Next: []cid.Cid{prev[0].GetHash()}}, nil)
				require.NoError(t, err)

				curr[1], err = entry.CreateEntry(ctx, dag, identities[1], &entry.Entry{Payload: []byte(fmt.Sprintf("entryB%d", i)), LogID: "X", Next: []cid.Cid{prev[1].GetHash(), curr[0].GetHash()}}, nil)
				require.NoError(t, err)

				curr[2], err = entry.CreateEntry(ctx, dag, identities[2], &entry.Entry{Payload: []byte(fmt.Sprintf("entryC%d", i)), LogID: "X", Next: []cid.Cid{prev[2].GetHash(), curr[0].GetHash(), curr[1].GetHash()}}, nil)
				require.NoError(t, err)
			}

			for j := 0; j < 3; j++ {
				items[j] = append(items[j], curr[j])
			}
		}

		// Here we're creating a log from entries signed by A and B
		// but we accept entries from C too
		logA, err := ipfslog.NewFromEntry(ctx, dag, identities[2], []iface.IPFSLogEntry{items[1][len(items[1])-1]}, &ipfslog.LogOptions{}, &entry.FetchOptions{})
		require.NoError(t, err)
		// Here we're creating a log from entries signed by peer A, B and C
		// "logA" accepts entries from peer C so we can join logs A and B
		logB, err := ipfslog.NewFromEntry(ctx, dag, identities[2], []iface.IPFSLogEntry{items[2][len(items[2])-1]}, &ipfslog.LogOptions{}, &entry.FetchOptions{})
		require.NoError(t, err)

		_, err = logA.Join(logB, -1)
		require.NoError(t, err)

		// The last entry, 'entryC100', should be the only head
		// (it points to entryB100, entryB100 and entryC99)
		require.Equal(t, len(entry.FindHeads(logA.Entries)), 1)
	})

	t.Run("returns error if log parameter is not defined", func(t *testing.T) {
		setup(t)
		_, err := logs[0].Join(nil, -1)
		require.Equal(t, err, errmsg.ErrLogJoinNotDefined)
	})

	t.Run("joins only unique items", func(t *testing.T) {
		setup(t)

		_, err := logs[0].Append(ctx, []byte("helloA1"), nil)
		require.NoError(t, err)

		_, err = logs[0].Append(ctx, []byte("helloA2"), nil)
		require.NoError(t, err)

		_, err = logs[1].Append(ctx, []byte("helloB1"), nil)
		require.NoError(t, err)

		_, err = logs[1].Append(ctx, []byte("helloB2"), nil)
		require.NoError(t, err)

		_, err = logs[0].Join(logs[1], -1)
		require.NoError(t, err)

		_, err = logs[0].Join(logs[1], -1)
		require.NoError(t, err)

		require.Equal(t, logs[0].Values().Len(), 4)

		expected := []string{"helloA1", "helloB1", "helloA2", "helloB2"}
		var result []string

		for _, v := range logs[0].Values().Keys() {
			result = append(result, string(logs[0].Values().UnsafeGet(v).GetPayload()))
		}

		require.Equal(t, expected, result)
		require.Equal(t, len(getLastEntry(logs[0].Values()).GetNext()), 1)
	})

	t.Run("joins logs two ways", func(t *testing.T) {
		setup(t)

		_, err := logs[0].Append(ctx, []byte("helloA1"), nil)
		require.NoError(t, err)

		_, err = logs[0].Append(ctx, []byte("helloA2"), nil)
		require.NoError(t, err)

		_, err = logs[1].Append(ctx, []byte("helloB1"), nil)
		require.NoError(t, err)

		_, err = logs[1].Append(ctx, []byte("helloB2"), nil)
		require.NoError(t, err)

		_, err = logs[0].Join(logs[1], -1)
		require.NoError(t, err)

		_, err = logs[1].Join(logs[0], -1)
		require.NoError(t, err)

		var hashes [2][]cid.Cid
		var payloads [2][][]byte
		expected := [][]byte{[]byte("helloA1"), []byte("helloB1"), []byte("helloA2"), []byte("helloB2")}

		for i := 0; i < 2; i++ {
			values := logs[i].Values()
			keys := values.Keys()
			for _, k := range keys {
				v := values.UnsafeGet(k)
				hashes[i] = append(hashes[i], v.GetHash())
				payloads[i] = append(payloads[i], v.GetPayload())
			}
		}

		require.True(t, reflect.DeepEqual(hashes[0], hashes[1]))
		require.Equal(t, payloads[0], expected)
		require.Equal(t, payloads[1], expected)
	})

	t.Run("joins logs twice", func(t *testing.T) {
		setup(t)

		_, err := logs[0].Append(ctx, []byte("helloA1"), nil)
		require.NoError(t, err)

		_, err = logs[1].Append(ctx, []byte("helloB1"), nil)
		require.NoError(t, err)

		_, err = logs[1].Join(logs[0], -1)
		require.NoError(t, err)

		_, err = logs[0].Append(ctx, []byte("helloA2"), nil)
		require.NoError(t, err)

		_, err = logs[1].Append(ctx, []byte("helloB2"), nil)
		require.NoError(t, err)

		_, err = logs[1].Join(logs[0], -1)
		require.NoError(t, err)

		require.Equal(t, logs[1].Values().Len(), 4)

		expected := []string{"helloA1", "helloB1", "helloA2", "helloB2"}
		var result []string

		for _, v := range logs[1].Values().Keys() {
			result = append(result, string(logs[1].Values().UnsafeGet(v).GetPayload()))
		}

		require.Equal(t, expected, result)
	})

	t.Run("joins 2 logs two ways", func(t *testing.T) {
		setup(t)

		_, err := logs[0].Append(ctx, []byte("helloA1"), nil)
		require.NoError(t, err)
		_, err = logs[1].Append(ctx, []byte("helloB1"), nil)
		require.NoError(t, err)
		_, err = logs[1].Join(logs[0], -1)
		require.NoError(t, err)
		_, err = logs[0].Join(logs[1], -1)
		require.NoError(t, err)

		_, err = logs[0].Append(ctx, []byte("helloA2"), nil)
		require.NoError(t, err)
		_, err = logs[1].Append(ctx, []byte("helloB2"), nil)
		require.NoError(t, err)
		_, err = logs[1].Join(logs[0], -1)
		require.NoError(t, err)

		require.Equal(t, logs[1].Values().Len(), 4)

		expected := []string{"helloA1", "helloB1", "helloA2", "helloB2"}
		var result []string

		for _, v := range logs[1].Values().Keys() {
			result = append(result, string(logs[1].Values().UnsafeGet(v).GetPayload()))
		}

		require.Equal(t, expected, result)
	})

	t.Run("joins 2 logs two ways and has the right heads at every step", func(t *testing.T) {
		setup(t)

		_, err = logs[0].Append(ctx, []byte("helloA1"), nil)
		require.NoError(t, err)

		require.Equal(t, logs[0].Heads().Len(), 1)
		require.NotNil(t, logs[0].Heads().At(0))
		require.Equal(t, logs[0].Heads().At(0).GetPayload(), []byte("helloA1"))

		_, err = logs[1].Append(ctx, []byte("helloB1"), nil)
		require.NoError(t, err)

		require.Equal(t, logs[1].Heads().Len(), 1)
		require.NotNil(t, logs[1].Heads().At(0))
		require.Equal(t, logs[1].Heads().At(0).GetPayload(), []byte("helloB1"))

		_, err = logs[1].Join(logs[0], -1)
		require.NoError(t, err)

		require.Equal(t, logs[1].Heads().Len(), 2)
		require.NotNil(t, logs[1].Heads().At(0))
		require.Equal(t, logs[1].Heads().At(0).GetPayload(), []byte("helloB1"))
		require.NotNil(t, logs[1].Heads().At(1))
		require.Equal(t, logs[1].Heads().At(1).GetPayload(), []byte("helloA1"))

		_, err = logs[0].Join(logs[1], -1)
		require.NoError(t, err)

		require.Equal(t, logs[0].Heads().Len(), 2)
		require.NotNil(t, logs[0].Heads().At(0))
		require.Equal(t, logs[0].Heads().At(0).GetPayload(), []byte("helloB1"))
		require.NotNil(t, logs[0].Heads().At(1))
		require.Equal(t, logs[0].Heads().At(1).GetPayload(), []byte("helloA1"))

		_, err = logs[0].Append(ctx, []byte("helloA2"), nil)
		require.NoError(t, err)

		require.Equal(t, logs[0].Heads().Len(), 1)
		require.NotNil(t, logs[0].Heads().At(0))
		require.Equal(t, logs[0].Heads().At(0).GetPayload(), []byte("helloA2"))

		_, err = logs[1].Append(ctx, []byte("helloB2"), nil)
		require.NoError(t, err)

		require.Equal(t, logs[1].Heads().Len(), 1)
		require.NotNil(t, logs[1].Heads().At(0))
		require.Equal(t, logs[1].Heads().At(0).GetPayload(), []byte("helloB2"))

		_, err = logs[1].Join(logs[0], -1)
		require.NoError(t, err)

		require.Equal(t, logs[1].Heads().Len(), 2)
		require.NotNil(t, logs[1].Heads().At(0))
		require.Equal(t, logs[1].Heads().At(0).GetPayload(), []byte("helloB2"))
		require.NotNil(t, logs[1].Heads().At(1))
		require.Equal(t, logs[1].Heads().At(1).GetPayload(), []byte("helloA2"))
	})

	t.Run("joins 4 logs to one", func(t *testing.T) {
		setup(t)

		// order determined by identity's publicKey
		_, err := logs[0].Append(ctx, []byte("helloA1"), nil)
		require.NoError(t, err)
		_, err = logs[0].Append(ctx, []byte("helloA2"), nil)
		require.NoError(t, err)

		_, err = logs[2].Append(ctx, []byte("helloB1"), nil)
		require.NoError(t, err)
		_, err = logs[2].Append(ctx, []byte("helloB2"), nil)
		require.NoError(t, err)

		_, err = logs[1].Append(ctx, []byte("helloC1"), nil)
		require.NoError(t, err)
		_, err = logs[1].Append(ctx, []byte("helloC2"), nil)
		require.NoError(t, err)

		_, err = logs[3].Append(ctx, []byte("helloD1"), nil)
		require.NoError(t, err)
		_, err = logs[3].Append(ctx, []byte("helloD2"), nil)
		require.NoError(t, err)

		_, err = logs[0].Join(logs[1], -1)
		require.NoError(t, err)
		_, err = logs[0].Join(logs[2], -1)
		require.NoError(t, err)
		_, err = logs[0].Join(logs[3], -1)
		require.NoError(t, err)

		expected := []string{
			"helloA1",
			"helloB1",
			"helloC1",
			"helloD1",
			"helloA2",
			"helloB2",
			"helloC2",
			"helloD2",
		}

		require.Equal(t, logs[0].Values().Len(), 8)

		var result []string

		for _, v := range logs[0].Values().Keys() {
			result = append(result, string(logs[0].Values().UnsafeGet(v).GetPayload()))
		}

		require.Equal(t, expected, result)
	})

	t.Run("joins 4 logs to one is commutative", func(t *testing.T) {
		setup(t)

		_, err := logs[0].Append(ctx, []byte("helloA1"), nil)
		require.NoError(t, err)
		_, err = logs[0].Append(ctx, []byte("helloA2"), nil)
		require.NoError(t, err)

		_, err = logs[1].Append(ctx, []byte("helloB1"), nil)
		require.NoError(t, err)
		_, err = logs[1].Append(ctx, []byte("helloB2"), nil)
		require.NoError(t, err)

		_, err = logs[2].Append(ctx, []byte("helloC1"), nil)
		require.NoError(t, err)
		_, err = logs[2].Append(ctx, []byte("helloC2"), nil)
		require.NoError(t, err)

		_, err = logs[3].Append(ctx, []byte("helloD1"), nil)
		require.NoError(t, err)
		_, err = logs[3].Append(ctx, []byte("helloD2"), nil)
		require.NoError(t, err)

		_, err = logs[0].Join(logs[1], -1)
		require.NoError(t, err)
		_, err = logs[0].Join(logs[2], -1)
		require.NoError(t, err)
		_, err = logs[0].Join(logs[3], -1)
		require.NoError(t, err)

		_, err = logs[1].Join(logs[0], -1)
		require.NoError(t, err)
		_, err = logs[1].Join(logs[2], -1)
		require.NoError(t, err)
		_, err = logs[1].Join(logs[3], -1)
		require.NoError(t, err)

		require.Equal(t, logs[0].Values().Len(), 8)

		var payloads [2][]string

		for i := 0; i < 2; i++ {
			for _, v := range logs[i].Values().Keys() {
				payloads[i] = append(payloads[i], string(logs[i].Values().UnsafeGet(v).GetPayload()))
			}
		}

		require.Equal(t, payloads[0], payloads[1])
	})

	t.Run("joins logs and updates clocks", func(t *testing.T) {
		setup(t)

		_, err := logs[0].Append(ctx, []byte("helloA1"), nil)
		require.NoError(t, err)
		_, err = logs[1].Append(ctx, []byte("helloB1"), nil)
		require.NoError(t, err)
		_, err = logs[1].Join(logs[0], -1)
		require.NoError(t, err)
		_, err = logs[0].Append(ctx, []byte("helloA2"), nil)
		require.NoError(t, err)
		_, err = logs[1].Append(ctx, []byte("helloB2"), nil)
		require.NoError(t, err)

		require.Equal(t, logs[0].Clock.GetID(), identities[0].PublicKey)
		require.Equal(t, logs[1].Clock.GetID(), identities[1].PublicKey)
		require.Equal(t, logs[0].Clock.GetTime(), 2)
		require.Equal(t, logs[1].Clock.GetTime(), 2)

		_, err = logs[2].Join(logs[0], -1)
		require.NoError(t, err)
		require.Equal(t, logs[2].ID, "X")
		require.Equal(t, logs[2].Clock.GetID(), identities[2].PublicKey)
		require.Equal(t, logs[2].Clock.GetTime(), 2)

		_, err = logs[2].Append(ctx, []byte("helloC1"), nil)
		require.NoError(t, err)
		_, err = logs[2].Append(ctx, []byte("helloC2"), nil)
		require.NoError(t, err)
		_, err = logs[0].Join(logs[2], -1)
		require.NoError(t, err)
		_, err = logs[0].Join(logs[1], -1)
		require.NoError(t, err)
		_, err = logs[3].Append(ctx, []byte("helloD1"), nil)
		require.NoError(t, err)
		_, err = logs[3].Append(ctx, []byte("helloD2"), nil)
		require.NoError(t, err)
		_, err = logs[3].Join(logs[1], -1)
		require.NoError(t, err)
		_, err = logs[3].Join(logs[0], -1)
		require.NoError(t, err)
		_, err = logs[3].Join(logs[2], -1)
		require.NoError(t, err)
		_, err = logs[3].Append(ctx, []byte("helloD3"), nil)
		require.NoError(t, err)
		_, err = logs[3].Append(ctx, []byte("helloD4"), nil)
		require.NoError(t, err)

		_, err = logs[0].Join(logs[3], -1)
		require.NoError(t, err)
		_, err = logs[3].Join(logs[0], -1)
		require.NoError(t, err)
		_, err = logs[3].Append(ctx, []byte("helloD5"), nil)
		require.NoError(t, err)
		_, err = logs[0].Append(ctx, []byte("helloA5"), nil)
		require.NoError(t, err)
		_, err = logs[3].Join(logs[0], -1)
		require.NoError(t, err)
		require.Equal(t, logs[3].Clock.GetID(), identities[3].PublicKey)
		require.Equal(t, logs[3].Clock.GetTime(), 7)

		_, err = logs[3].Append(ctx, []byte("helloD6"), nil)
		require.NoError(t, err)
		require.Equal(t, logs[3].Clock.GetTime(), 8)

		expected := []entry.Entry{
			{Payload: []byte("helloA1"), LogID: "X", Clock: &entry.LamportClock{ID: identities[0].PublicKey, Time: 1}},
			{Payload: []byte("helloB1"), LogID: "X", Clock: &entry.LamportClock{ID: identities[1].PublicKey, Time: 1}},
			{Payload: []byte("helloD1"), LogID: "X", Clock: &entry.LamportClock{ID: identities[3].PublicKey, Time: 1}},
			{Payload: []byte("helloA2"), LogID: "X", Clock: &entry.LamportClock{ID: identities[0].PublicKey, Time: 2}},
			{Payload: []byte("helloB2"), LogID: "X", Clock: &entry.LamportClock{ID: identities[1].PublicKey, Time: 2}},
			{Payload: []byte("helloD2"), LogID: "X", Clock: &entry.LamportClock{ID: identities[3].PublicKey, Time: 2}},
			{Payload: []byte("helloC1"), LogID: "X", Clock: &entry.LamportClock{ID: identities[2].PublicKey, Time: 3}},
			{Payload: []byte("helloC2"), LogID: "X", Clock: &entry.LamportClock{ID: identities[2].PublicKey, Time: 4}},
			{Payload: []byte("helloD3"), LogID: "X", Clock: &entry.LamportClock{ID: identities[3].PublicKey, Time: 5}},
			{Payload: []byte("helloD4"), LogID: "X", Clock: &entry.LamportClock{ID: identities[3].PublicKey, Time: 6}},
			{Payload: []byte("helloA5"), LogID: "X", Clock: &entry.LamportClock{ID: identities[0].PublicKey, Time: 7}},
			{Payload: []byte("helloD5"), LogID: "X", Clock: &entry.LamportClock{ID: identities[3].PublicKey, Time: 7}},
			{Payload: []byte("helloD6"), LogID: "X", Clock: &entry.LamportClock{ID: identities[3].PublicKey, Time: 8}},
		}

		require.Equal(t, logs[3].Values().Len(), 13)

		var result []entry.Entry

		for _, v := range logs[3].Values().Keys() {
			e, exist := logs[3].Values().Get(v)
			require.True(t, exist)
			result = append(result, entry.Entry{Payload: e.GetPayload(), LogID: e.GetLogID(), Clock: &entry.LamportClock{
				ID:   e.GetClock().GetID(),
				Time: e.GetClock().GetTime(),
			}})
		}

		require.True(t, reflect.DeepEqual(result, expected))
	})

	t.Run("joins logs from 4 logs", func(t *testing.T) {
		setup(t)

		_, err := logs[0].Append(ctx, []byte("helloA1"), nil)
		require.NoError(t, err)
		_, err = logs[0].Join(logs[1], -1)
		require.NoError(t, err)
		_, err = logs[1].Append(ctx, []byte("helloB1"), nil)
		require.NoError(t, err)
		_, err = logs[1].Join(logs[0], -1)
		require.NoError(t, err)
		_, err = logs[0].Append(ctx, []byte("helloA2"), nil)
		require.NoError(t, err)
		_, err = logs[1].Append(ctx, []byte("helloB2"), nil)
		require.NoError(t, err)

		_, err = logs[0].Join(logs[2], -1)
		require.NoError(t, err)
		require.Equal(t, logs[0].ID, "X")
		require.Equal(t, logs[0].Clock.GetID(), identities[0].PublicKey)
		require.Equal(t, logs[0].Clock.GetTime(), 2)

		_, err = logs[2].Join(logs[0], -1)
		require.NoError(t, err)
		require.Equal(t, logs[2].ID, "X")
		require.Equal(t, logs[2].Clock.GetID(), identities[2].PublicKey)
		require.Equal(t, logs[2].Clock.GetTime(), 2)

		_, err = logs[2].Append(ctx, []byte("helloC1"), nil)
		require.NoError(t, err)
		_, err = logs[2].Append(ctx, []byte("helloC2"), nil)
		require.NoError(t, err)
		_, err = logs[0].Join(logs[2], -1)
		require.NoError(t, err)
		_, err = logs[0].Join(logs[1], -1)
		require.NoError(t, err)
		_, err = logs[3].Append(ctx, []byte("helloD1"), nil)
		require.NoError(t, err)
		_, err = logs[3].Append(ctx, []byte("helloD2"), nil)
		require.NoError(t, err)
		_, err = logs[3].Join(logs[1], -1)
		require.NoError(t, err)
		_, err = logs[3].Join(logs[0], -1)
		require.NoError(t, err)
		_, err = logs[3].Join(logs[2], -1)
		require.NoError(t, err)
		_, err = logs[3].Append(ctx, []byte("helloD3"), nil)
		require.NoError(t, err)
		_, err = logs[3].Append(ctx, []byte("helloD4"), nil)
		require.NoError(t, err)

		require.Equal(t, logs[3].Clock.GetID(), identities[3].PublicKey)
		require.Equal(t, logs[3].Clock.GetTime(), 6)

		expected := [][]byte{
			[]byte("helloA1"),
			[]byte("helloB1"),
			[]byte("helloD1"),
			[]byte("helloA2"),
			[]byte("helloB2"),
			[]byte("helloD2"),
			[]byte("helloC1"),
			[]byte("helloC2"),
			[]byte("helloD3"),
			[]byte("helloD4"),
		}

		require.Equal(t, logs[3].Values().Len(), 10)

		var result [][]byte

		for _, v := range logs[3].Values().Keys() {
			result = append(result, logs[3].Values().UnsafeGet(v).GetPayload())
		}

		require.True(t, reflect.DeepEqual(expected, result))
	})

	t.Run("joins only specified amount of entries - one entry", func(t *testing.T) {
		setup(t)

		_, err := logs[0].Append(ctx, []byte("helloA1"), nil)
		require.NoError(t, err)
		_, err = logs[0].Append(ctx, []byte("helloA2"), nil)
		require.NoError(t, err)
		_, err = logs[1].Append(ctx, []byte("helloB1"), nil)
		require.NoError(t, err)
		_, err = logs[1].Append(ctx, []byte("helloB2"), nil)
		require.NoError(t, err)

		_, err = logs[0].Join(logs[1], 1)
		require.NoError(t, err)

		expected := [][]byte{[]byte("helloB2")}

		require.Equal(t, logs[0].Values().Len(), 1)

		var result [][]byte
		var key string

		for _, v := range logs[0].Values().Keys() {
			result = append(result, logs[0].Values().UnsafeGet(v).GetPayload())
			key = v
		}

		require.True(t, reflect.DeepEqual(expected, result))
		require.Equal(t, len(logs[0].Values().UnsafeGet(key).GetNext()), 1)
	})

	t.Run("joins only specified amount of entries - two entries", func(t *testing.T) {
		setup(t)

		_, err := logs[0].Append(ctx, []byte("helloA1"), nil)
		require.NoError(t, err)
		_, err = logs[0].Append(ctx, []byte("helloA2"), nil)
		require.NoError(t, err)
		_, err = logs[1].Append(ctx, []byte("helloB1"), nil)
		require.NoError(t, err)
		_, err = logs[1].Append(ctx, []byte("helloB2"), nil)
		require.NoError(t, err)

		_, err = logs[0].Join(logs[1], 2)
		require.NoError(t, err)

		expected := [][]byte{[]byte("helloA2"), []byte("helloB2")}

		require.Equal(t, logs[0].Values().Len(), 2)

		var result [][]byte
		var key string

		for _, v := range logs[0].Values().Keys() {
			result = append(result, logs[0].Values().UnsafeGet(v).GetPayload())
			key = v
		}

		require.True(t, reflect.DeepEqual(expected, result))
		require.Equal(t, len(logs[0].Values().UnsafeGet(key).GetNext()), 1)
	})

	t.Run("joins only specified amount of entries - three entries", func(t *testing.T) {
		setup(t)

		_, err := logs[0].Append(ctx, []byte("helloA1"), nil)
		require.NoError(t, err)
		_, err = logs[0].Append(ctx, []byte("helloA2"), nil)
		require.NoError(t, err)
		_, err = logs[1].Append(ctx, []byte("helloB1"), nil)
		require.NoError(t, err)
		_, err = logs[1].Append(ctx, []byte("helloB2"), nil)
		require.NoError(t, err)

		_, err = logs[0].Join(logs[1], 3)
		require.NoError(t, err)

		expected := [][]byte{[]byte("helloB1"), []byte("helloA2"), []byte("helloB2")}

		require.Equal(t, logs[0].Values().Len(), 3)

		var result [][]byte
		var key string

		for _, v := range logs[0].Values().Keys() {
			result = append(result, logs[0].Values().UnsafeGet(v).GetPayload())
			key = v
		}

		require.True(t, reflect.DeepEqual(expected, result))
		require.Equal(t, len(logs[0].Values().UnsafeGet(key).GetNext()), 1)
	})

	t.Run("joins only specified amount of entries - (all) four entries", func(t *testing.T) {
		setup(t)

		_, err := logs[0].Append(ctx, []byte("helloA1"), nil)
		require.NoError(t, err)
		_, err = logs[0].Append(ctx, []byte("helloA2"), nil)
		require.NoError(t, err)
		_, err = logs[1].Append(ctx, []byte("helloB1"), nil)
		require.NoError(t, err)
		_, err = logs[1].Append(ctx, []byte("helloB2"), nil)
		require.NoError(t, err)

		_, err = logs[0].Join(logs[1], 4)
		require.NoError(t, err)

		expected := [][]byte{[]byte("helloA1"), []byte("helloB1"), []byte("helloA2"), []byte("helloB2")}

		require.Equal(t, logs[0].Values().Len(), 4)

		var result [][]byte
		var key string

		for _, v := range logs[0].Values().Keys() {
			result = append(result, logs[0].Values().UnsafeGet(v).GetPayload())
			key = v
		}

		require.True(t, reflect.DeepEqual(expected, result))
		require.Equal(t, len(logs[0].Values().UnsafeGet(key).GetNext()), 1)
	})
}
