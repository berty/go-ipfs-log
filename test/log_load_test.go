package test

import (
	"bytes"
	"context"
	"fmt"
	"math/rand"
	_ "sort"
	"strings"
	"testing"
	"time"

	"berty.tech/go-ipfs-log/io/pb"

	ipfslog "berty.tech/go-ipfs-log"
	"berty.tech/go-ipfs-log/entry"
	"berty.tech/go-ipfs-log/entry/sorting"
	"berty.tech/go-ipfs-log/errmsg"
	idp "berty.tech/go-ipfs-log/identityprovider"
	"berty.tech/go-ipfs-log/iface"
	ks "berty.tech/go-ipfs-log/keystore"
	cid "github.com/ipfs/go-cid"
	dssync "github.com/ipfs/go-datastore/sync"
	mocknet "github.com/libp2p/go-libp2p/p2p/net/mock"
	"github.com/stretchr/testify/require"
)

func BadComparatorReturnsZero(a, b iface.IPFSLogEntry) (int, error) {
	return 0, nil
}

func TestLogLoad(t *testing.T) {
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

	identities := make([]*idp.Identity, 4)

	for i, char := range []rune{'C', 'B', 'D', 'A'} {
		identity, err := idp.CreateIdentity(ctx, &idp.CreateIdentityOptions{
			Keystore: keystore,
			ID:       fmt.Sprintf("user%c", char),
			Type:     "orbitdb",
		})
		require.NoError(t, err)

		identities[i] = identity
	}

	firstWriteExpectedData := []string{
		"entryA6", "entryA7", "entryA8", "entryA9",
		"entryA10", "entryB1", "entryB2", "entryB3",
		"entryB4", "entryB5", "entryA1", "entryA2",
		"entryA3", "entryA4", "entryA5", "entryC0",
	}

	_ = firstWriteExpectedData

	t.Run("fromJSON", func(t *testing.T) {
		t.Run("creates a log from an entry", func(t *testing.T) {
			fixture, err := CreateLogWithSixteenEntries(ctx, dag, identities)
			require.NoError(t, err)

			data := fixture.Log
			json := fixture.JSON

			// TODO: Is this useless?
			//heads := map[string]*entry.Entry{}
			//
			//for _, h := range json.Heads {
			//	e, err := entry.fromMultihash(dag, h, identities[0].Provider)
			//	require.NoError(t, err)
			//
			//	heads[e.Hash.String()] = e
			//}

			l, err := ipfslog.NewFromJSON(ctx, dag, identities[0], json, &ipfslog.LogOptions{ID: "X"}, &entry.FetchOptions{})
			require.NoError(t, err)

			values := l.Values()

			require.Equal(t, l.ID, data.Heads().At(0).GetLogID())
			require.Equal(t, values.Len(), 16)
			require.Equal(t, entriesAsStrings(values), fixture.ExpectedData)
		})

		t.Run("creates a log from an entry with custom tiebreaker", func(t *testing.T) {
			fixture, err := CreateLogWithSixteenEntries(ctx, dag, identities)
			require.NoError(t, err)

			data := fixture.Log
			json := fixture.JSON

			l, err := ipfslog.NewFromJSON(ctx, dag, identities[0], json, &ipfslog.LogOptions{ID: "X", SortFn: sorting.FirstWriteWins}, &entry.FetchOptions{Length: intPtr(-1)})
			require.NoError(t, err)

			require.Equal(t, l.ID, data.Heads().At(0).GetLogID())
			require.Equal(t, l.Values().Len(), 16)
			// TODO: found out why firstWriteExpectedData is what it is in JS test

			require.Equal(t, entriesAsStrings(l.Values()), firstWriteExpectedData)
			_ = firstWriteExpectedData
		})
	})

	t.Run("fromEntryHash", func(t *testing.T) {
		t.Run("creates a log from an entry hash", func(t *testing.T) {
			fixture, err := CreateLogWithSixteenEntries(ctx, dag, identities)
			require.NoError(t, err)

			data := fixture.Log
			json := fixture.JSON

			log1, err := ipfslog.NewFromEntryHash(ctx, dag, identities[0], json.Heads[0], &ipfslog.LogOptions{ID: "X"}, &ipfslog.FetchOptions{})
			log2, err := ipfslog.NewFromEntryHash(ctx, dag, identities[0], json.Heads[1], &ipfslog.LogOptions{ID: "X"}, &ipfslog.FetchOptions{})

			_, err = log1.Join(log2, -1)
			require.NoError(t, err)

			require.Equal(t, log1.ID, data.Heads().At(0).GetLogID())
			require.Equal(t, log1.Values().Len(), 16)
			require.Equal(t, entriesAsStrings(log1.Values()), fixture.ExpectedData)
		})

		t.Run("creates a log from an entry hash with custom tiebreaker", func(t *testing.T) {
			fixture, err := CreateLogWithSixteenEntries(ctx, dag, identities)
			require.NoError(t, err)

			data := fixture.Log
			json := fixture.JSON

			log1, err := ipfslog.NewFromEntryHash(ctx, dag, identities[0], json.Heads[0], &ipfslog.LogOptions{ID: "X", SortFn: sorting.FirstWriteWins}, &ipfslog.FetchOptions{})
			log2, err := ipfslog.NewFromEntryHash(ctx, dag, identities[0], json.Heads[1], &ipfslog.LogOptions{ID: "X", SortFn: sorting.FirstWriteWins}, &ipfslog.FetchOptions{})

			_, err = log1.Join(log2, -1)
			require.NoError(t, err)

			require.Equal(t, log1.ID, data.Heads().At(0).GetLogID())
			require.Equal(t, log1.Values().Len(), 16)
			require.Equal(t, entriesAsStrings(log1.Values()), firstWriteExpectedData)
		})
	})

	t.Run("fromEntry", func(t *testing.T) {
		resortedIdentities := []*idp.Identity{identities[2], identities[1], identities[0], identities[3]}

		t.Run("creates a log from an entry", func(t *testing.T) {
			fixture, err := CreateLogWithSixteenEntries(ctx, dag, resortedIdentities)
			require.NoError(t, err)

			data := fixture.Log

			l, err := ipfslog.NewFromEntry(ctx, dag, identities[0], data.Heads().Slice(), &ipfslog.LogOptions{}, &entry.FetchOptions{})
			require.NoError(t, err)

			require.Equal(t, l.ID, data.Heads().At(0).GetLogID())
			require.Equal(t, l.Values().Len(), 16)
			require.Equal(t, entriesAsStrings(l.Values()), fixture.ExpectedData)
		})

		t.Run("creates a log from an entry with custom tiebreaker", func(t *testing.T) {
			fixture, err := CreateLogWithSixteenEntries(ctx, dag, resortedIdentities)
			require.NoError(t, err)

			data := fixture.Log

			l, err := ipfslog.NewFromEntry(ctx, dag, identities[0], data.Heads().Slice(), &ipfslog.LogOptions{SortFn: sorting.FirstWriteWins}, &entry.FetchOptions{Length: intPtr(-1)})
			require.NoError(t, err)

			require.Equal(t, l.ID, data.Heads().At(0).GetLogID())
			require.Equal(t, l.Values().Len(), 16)
			require.Equal(t, entriesAsStrings(l.Values()), firstWriteExpectedData)
		})

		t.Run("keeps the original heads", func(t *testing.T) {
			fixture, err := CreateLogWithSixteenEntries(ctx, dag, resortedIdentities)
			require.NoError(t, err)

			data := fixture.Log

			log1, err := ipfslog.NewFromEntry(ctx, dag, identities[0], data.Heads().Slice(), &ipfslog.LogOptions{}, &entry.FetchOptions{Length: intPtr(data.Heads().Len())})

			require.NoError(t, err)
			require.Equal(t, log1.ID, data.Heads().At(0).GetLogID())
			require.Equal(t, log1.Values().Len(), data.Heads().Len())
			require.Equal(t, string(log1.Values().At(0).GetPayload()), "entryC0")
			require.Equal(t, string(log1.Values().At(1).GetPayload()), "entryA10")

			log2, err := ipfslog.NewFromEntry(ctx, dag, identities[0], data.Heads().Slice(), &ipfslog.LogOptions{}, &entry.FetchOptions{Length: intPtr(4)})

			require.NoError(t, err)
			require.Equal(t, log2.ID, data.Heads().At(0).GetLogID())
			require.Equal(t, log2.Values().Len(), 4)
			require.Equal(t, string(log2.Values().At(0).GetPayload()), "entryC0")
			require.Equal(t, string(log2.Values().At(1).GetPayload()), "entryA8")
			require.Equal(t, string(log2.Values().At(2).GetPayload()), "entryA9")
			require.Equal(t, string(log2.Values().At(3).GetPayload()), "entryA10")

			log3, err := ipfslog.NewFromEntry(ctx, dag, identities[0], data.Heads().Slice(), &ipfslog.LogOptions{}, &entry.FetchOptions{Length: intPtr(7)})

			require.NoError(t, err)
			require.Equal(t, log3.ID, data.Heads().At(0).GetLogID())
			require.Equal(t, log3.Values().Len(), 7)
			require.Equal(t, string(log3.Values().At(0).GetPayload()), "entryB5")
			require.Equal(t, string(log3.Values().At(1).GetPayload()), "entryA6")
			require.Equal(t, string(log3.Values().At(2).GetPayload()), "entryC0")
			require.Equal(t, string(log3.Values().At(3).GetPayload()), "entryA7")
			require.Equal(t, string(log3.Values().At(4).GetPayload()), "entryA8")
			require.Equal(t, string(log3.Values().At(5).GetPayload()), "entryA9")
			require.Equal(t, string(log3.Values().At(6).GetPayload()), "entryA10")
		})

		t.Run("onProgress callback is fired for each entry", func(t *testing.T) {
			// TODO: skipped
		})

		t.Run("retrieves partial log from an entry hash", func(t *testing.T) {
			log1, err := ipfslog.NewLog(dag, identities[0], &ipfslog.LogOptions{ID: "X"})
			require.NoError(t, err)

			log2, err := ipfslog.NewLog(dag, identities[0], &ipfslog.LogOptions{ID: "X"})
			require.NoError(t, err)

			log3, err := ipfslog.NewLog(dag, identities[0], &ipfslog.LogOptions{ID: "X"})
			require.NoError(t, err)

			var items1 []iface.IPFSLogEntry
			var items2 []iface.IPFSLogEntry
			var items3 []iface.IPFSLogEntry

			const amount = 100
			for i := 1; i <= amount; i++ {
				prev1 := lastEntry(items1)
				prev2 := lastEntry(items2)
				prev3 := lastEntry(items3)

				var nexts []cid.Cid
				if prev1 != nil {
					nexts = []cid.Cid{prev1.GetHash()}
				}

				n1, err := entry.CreateEntry(ctx, dag, log1.Identity, &entry.Entry{LogID: "X", Payload: []byte(fmt.Sprintf("entryA%d", i)), Next: nexts}, nil)
				require.NoError(t, err)

				if prev2 != nil {
					nexts = []cid.Cid{prev2.GetHash(), n1.GetHash()}
				} else {
					nexts = []cid.Cid{n1.GetHash()}
				}

				n2, err := entry.CreateEntry(ctx, dag, log2.Identity, &entry.Entry{LogID: "X", Payload: []byte(fmt.Sprintf("entryB%d", i)), Next: nexts}, nil)
				require.NoError(t, err)

				if prev3 != nil {
					nexts = []cid.Cid{prev3.GetHash(), n1.GetHash(), n2.GetHash()}
				} else {
					nexts = []cid.Cid{n1.GetHash(), n2.GetHash()}
				}

				n3, err := entry.CreateEntry(ctx, dag, log3.Identity, &entry.Entry{LogID: "X", Payload: []byte(fmt.Sprintf("entryC%d", i)), Next: nexts}, nil)
				require.NoError(t, err)

				items1 = append(items1, n1)
				items2 = append(items2, n2)
				items3 = append(items3, n3)
			}

			// limit to 10 entries
			a, err := ipfslog.NewFromEntry(ctx, dag, identities[0], []iface.IPFSLogEntry{lastEntry(items1)}, &ipfslog.LogOptions{}, &entry.FetchOptions{Length: intPtr(10)})
			require.NoError(t, err)
			require.Equal(t, a.Values().Len(), 10)

			// limit to 42 entries
			b, err := ipfslog.NewFromEntry(ctx, dag, identities[0], []iface.IPFSLogEntry{lastEntry(items1)}, &ipfslog.LogOptions{}, &entry.FetchOptions{Length: intPtr(42)})
			require.NoError(t, err)
			require.Equal(t, b.Values().Len(), 42)
		})

		t.Run("retrieves full log from an entry hash", func(t *testing.T) {
			log1, err := ipfslog.NewLog(dag, identities[0], &ipfslog.LogOptions{ID: "X"})
			require.NoError(t, err)

			log2, err := ipfslog.NewLog(dag, identities[0], &ipfslog.LogOptions{ID: "X"})
			require.NoError(t, err)

			log3, err := ipfslog.NewLog(dag, identities[0], &ipfslog.LogOptions{ID: "X"})
			require.NoError(t, err)

			var items1 []iface.IPFSLogEntry
			var items2 []iface.IPFSLogEntry
			var items3 []iface.IPFSLogEntry

			const amount = 100
			for i := 1; i <= amount; i++ {
				prev1 := lastEntry(items1)
				prev2 := lastEntry(items2)
				prev3 := lastEntry(items3)

				var nexts []cid.Cid
				if prev1 != nil {
					nexts = []cid.Cid{prev1.GetHash()}
				}

				n1, err := entry.CreateEntry(ctx, dag, log1.Identity, &entry.Entry{LogID: "X", Payload: []byte(fmt.Sprintf("entryA%d", i)), Next: nexts}, nil)
				require.NoError(t, err)

				if prev2 != nil {
					nexts = []cid.Cid{prev2.GetHash(), n1.GetHash()}
				} else {
					nexts = []cid.Cid{n1.GetHash()}
				}

				n2, err := entry.CreateEntry(ctx, dag, log2.Identity, &entry.Entry{LogID: "X", Payload: []byte(fmt.Sprintf("entryB%d", i)), Next: nexts}, nil)
				require.NoError(t, err)

				if prev3 != nil {
					nexts = []cid.Cid{prev3.GetHash(), n2.GetHash()}
				} else {
					nexts = []cid.Cid{n2.GetHash()}
				}

				n3, err := entry.CreateEntry(ctx, dag, log3.Identity, &entry.Entry{LogID: "X", Payload: []byte(fmt.Sprintf("entryC%d", i)), Next: nexts}, nil)
				require.NoError(t, err)

				items1 = append(items1, n1)
				items2 = append(items2, n2)
				items3 = append(items3, n3)
			}

			lA, err := ipfslog.NewFromEntry(ctx, dag, identities[0], []iface.IPFSLogEntry{lastEntry(items1)}, &ipfslog.LogOptions{}, &entry.FetchOptions{Length: intPtr(amount * 1)})
			require.NoError(t, err)
			require.Equal(t, lA.Values().Len(), amount)

			lB, err := ipfslog.NewFromEntry(ctx, dag, identities[0], []iface.IPFSLogEntry{lastEntry(items2)}, &ipfslog.LogOptions{}, &entry.FetchOptions{Length: intPtr(amount * 2)})
			require.NoError(t, err)
			require.Equal(t, lB.Values().Len(), amount*2)

			lC, err := ipfslog.NewFromEntry(ctx, dag, identities[0], []iface.IPFSLogEntry{lastEntry(items3)}, &ipfslog.LogOptions{}, &entry.FetchOptions{Length: intPtr(amount * 3)})
			require.NoError(t, err)
			require.Equal(t, lC.Values().Len(), amount*3)
		})

		t.Run("retrieves full log from an entry hash 2", func(t *testing.T) {
			log1, err := ipfslog.NewLog(dag, identities[0], &ipfslog.LogOptions{ID: "X"})
			require.NoError(t, err)

			log2, err := ipfslog.NewLog(dag, identities[0], &ipfslog.LogOptions{ID: "X"})
			require.NoError(t, err)

			log3, err := ipfslog.NewLog(dag, identities[0], &ipfslog.LogOptions{ID: "X"})
			require.NoError(t, err)

			var items1 []iface.IPFSLogEntry
			var items2 []iface.IPFSLogEntry
			var items3 []iface.IPFSLogEntry

			const amount = 100
			for i := 1; i <= amount; i++ {
				prev1 := lastEntry(items1)
				prev2 := lastEntry(items2)
				prev3 := lastEntry(items3)

				var nexts []cid.Cid
				if prev1 != nil {
					nexts = []cid.Cid{prev1.GetHash()}
				}

				n1, err := entry.CreateEntry(ctx, dag, log1.Identity, &entry.Entry{LogID: "X", Payload: []byte(fmt.Sprintf("entryA%d", i)), Next: nexts}, nil)
				require.NoError(t, err)

				if prev2 != nil {
					nexts = []cid.Cid{prev2.GetHash(), n1.GetHash()}
				} else {
					nexts = []cid.Cid{n1.GetHash()}
				}

				n2, err := entry.CreateEntry(ctx, dag, log2.Identity, &entry.Entry{LogID: "X", Payload: []byte(fmt.Sprintf("entryB%d", i)), Next: nexts}, nil)
				require.NoError(t, err)

				if prev3 != nil {
					nexts = []cid.Cid{prev3.GetHash(), n1.GetHash(), n2.GetHash()}
				} else {
					nexts = []cid.Cid{n1.GetHash(), n2.GetHash()}
				}

				n3, err := entry.CreateEntry(ctx, dag, log3.Identity, &entry.Entry{LogID: "X", Payload: []byte(fmt.Sprintf("entryC%d", i)), Next: nexts}, nil)
				require.NoError(t, err)

				items1 = append(items1, n1)
				items2 = append(items2, n2)
				items3 = append(items3, n3)
			}

			lA, err := ipfslog.NewFromEntry(ctx, dag, identities[0], []iface.IPFSLogEntry{lastEntry(items1)}, &ipfslog.LogOptions{}, &entry.FetchOptions{Length: intPtr(amount * 1)})
			require.NoError(t, err)
			require.Equal(t, lA.Values().Len(), amount)

			lB, err := ipfslog.NewFromEntry(ctx, dag, identities[1], []iface.IPFSLogEntry{lastEntry(items2)}, &ipfslog.LogOptions{}, &entry.FetchOptions{Length: intPtr(amount * 2)})
			require.NoError(t, err)
			require.Equal(t, lB.Values().Len(), amount*2)

			lC, err := ipfslog.NewFromEntry(ctx, dag, identities[2], []iface.IPFSLogEntry{lastEntry(items3)}, &ipfslog.LogOptions{}, &entry.FetchOptions{Length: intPtr(amount * 3)})
			require.NoError(t, err)
			require.Equal(t, lC.Values().Len(), amount*3)
		})

		t.Run("retrieves full log from an entry hash 3", func(t *testing.T) {
			log1, err := ipfslog.NewLog(dag, identities[0], &ipfslog.LogOptions{ID: "X"})
			require.NoError(t, err)

			log2, err := ipfslog.NewLog(dag, identities[1], &ipfslog.LogOptions{ID: "X"})
			require.NoError(t, err)

			log3, err := ipfslog.NewLog(dag, identities[3], &ipfslog.LogOptions{ID: "X"})
			require.NoError(t, err)

			var items1 []iface.IPFSLogEntry
			var items2 []iface.IPFSLogEntry
			var items3 []iface.IPFSLogEntry

			const amount = 10
			for i := 1; i <= amount; i++ {
				prev1 := lastEntry(items1)
				prev2 := lastEntry(items2)
				prev3 := lastEntry(items3)

				var nexts []cid.Cid

				log1.Clock.Tick()
				log2.Clock.Tick()
				log3.Clock.Tick()

				if prev1 != nil {
					nexts = []cid.Cid{prev1.GetHash()}
				}

				n1, err := entry.CreateEntry(ctx, dag, log1.Identity, &entry.Entry{LogID: "X", Payload: []byte(fmt.Sprintf("entryA%d", i)), Next: nexts, Clock: entry.CopyLamportClock(log1.Clock)}, nil)
				require.NoError(t, err)

				if prev2 != nil {
					nexts = []cid.Cid{prev2.GetHash(), n1.GetHash()}
				} else {
					nexts = []cid.Cid{n1.GetHash()}
				}

				n2, err := entry.CreateEntry(ctx, dag, log2.Identity, &entry.Entry{LogID: "X", Payload: []byte(fmt.Sprintf("entryB%d", i)), Next: nexts, Clock: entry.CopyLamportClock(log2.Clock)}, nil)
				require.NoError(t, err)

				if prev3 != nil {
					nexts = []cid.Cid{prev3.GetHash(), n1.GetHash(), n2.GetHash()}
				} else {
					nexts = []cid.Cid{n1.GetHash(), n2.GetHash()}
				}

				n3, err := entry.CreateEntry(ctx, dag, log3.Identity, &entry.Entry{LogID: "X", Payload: []byte(fmt.Sprintf("entryC%d", i)), Next: nexts, Clock: entry.CopyLamportClock(log3.Clock)}, nil)
				require.NoError(t, err)

				log1.Clock.Merge(log2.Clock)
				log1.Clock.Merge(log3.Clock)
				log2.Clock.Merge(log1.Clock)
				log2.Clock.Merge(log3.Clock)
				log3.Clock.Merge(log1.Clock)
				log3.Clock.Merge(log2.Clock)

				items1 = append(items1, n1)
				items2 = append(items2, n2)
				items3 = append(items3, n3)
			}

			lA, err := ipfslog.NewFromEntry(ctx, dag, identities[0], []iface.IPFSLogEntry{lastEntry(items1)}, &ipfslog.LogOptions{}, &entry.FetchOptions{Length: intPtr(amount * 1)})
			require.NoError(t, err)
			require.Equal(t, lA.Values().Len(), amount)

			itemsInB := []string{
				"entryA1",
				"entryB1",
				"entryA2",
				"entryB2",
				"entryA3",
				"entryB3",
				"entryA4",
				"entryB4",
				"entryA5",
				"entryB5",
				"entryA6",
				"entryB6",
				"entryA7",
				"entryB7",
				"entryA8",
				"entryB8",
				"entryA9",
				"entryB9",
				"entryA10",
				"entryB10",
			}

			lB, err := ipfslog.NewFromEntry(ctx, dag, identities[1], []iface.IPFSLogEntry{lastEntry(items2)}, &ipfslog.LogOptions{}, &entry.FetchOptions{Length: intPtr(amount * 2)})
			require.NoError(t, err)
			require.Equal(t, lB.Values().Len(), amount*2)
			require.Equal(t, entriesAsStrings(lB.Values()), itemsInB)

			lC, err := ipfslog.NewFromEntry(ctx, dag, identities[3], []iface.IPFSLogEntry{lastEntry(items3)}, &ipfslog.LogOptions{}, &entry.FetchOptions{Length: intPtr(amount * 3)})
			require.NoError(t, err)

			_, err = lC.Append(ctx, []byte("EOF"), nil)
			require.NoError(t, err)

			require.Equal(t, lC.Values().Len(), amount*3+1)

			tmp := []string{
				"entryA1",
				"entryB1",
				"entryC1",
				"entryA2",
				"entryB2",
				"entryC2",
				"entryA3",
				"entryB3",
				"entryC3",
				"entryA4",
				"entryB4",
				"entryC4",
				"entryA5",
				"entryB5",
				"entryC5",
				"entryA6",
				"entryB6",
				"entryC6",
				"entryA7",
				"entryB7",
				"entryC7",
				"entryA8",
				"entryB8",
				"entryC8",
				"entryA9",
				"entryB9",
				"entryC9",
				"entryA10",
				"entryB10",
				"entryC10",
				"EOF",
			}

			require.Equal(t, entriesAsStrings(lC.Values()), tmp)

			// make sure logX comes after A, B and C
			logX, err := ipfslog.NewLog(dag, identities[3], &ipfslog.LogOptions{ID: "X"})
			require.NoError(t, err)

			_, err = logX.Append(ctx, []byte{'1'}, nil)
			require.NoError(t, err)

			_, err = logX.Append(ctx, []byte{'2'}, nil)
			require.NoError(t, err)

			_, err = logX.Append(ctx, []byte{'3'}, nil)
			require.NoError(t, err)

			lD, err := ipfslog.NewFromEntry(ctx, dag, identities[2], []iface.IPFSLogEntry{lastEntry(logX.Values().Slice())}, &ipfslog.LogOptions{}, &entry.FetchOptions{Length: intPtr(-1)})
			require.NoError(t, err)

			_, err = lC.Join(lD, -1)
			require.NoError(t, err)

			_, err = lD.Join(lC, -1)
			require.NoError(t, err)

			_, err = lC.Append(ctx, []byte("DONE"), nil)
			require.NoError(t, err)

			_, err = lD.Append(ctx, []byte("DONE"), nil)
			require.NoError(t, err)

			logF, err := ipfslog.NewFromEntry(ctx, dag, identities[2], []iface.IPFSLogEntry{lastEntry(lC.Values().Slice())}, &ipfslog.LogOptions{}, &entry.FetchOptions{Length: intPtr(-1), Exclude: nil})
			require.NoError(t, err)

			logG, err := ipfslog.NewFromEntry(ctx, dag, identities[2], []iface.IPFSLogEntry{lastEntry(lD.Values().Slice())}, &ipfslog.LogOptions{}, &entry.FetchOptions{Length: intPtr(-1), Exclude: nil})
			require.NoError(t, err)

			require.Equal(t, logF.ToString(nil), bigLogString)
			require.Equal(t, logG.ToString(nil), bigLogString)
		})

		t.Run("retrieves full log of randomly joined log", func(t *testing.T) {
			log1, err := ipfslog.NewLog(dag, identities[0], &ipfslog.LogOptions{ID: "X"})
			require.NoError(t, err)

			log2, err := ipfslog.NewLog(dag, identities[1], &ipfslog.LogOptions{ID: "X"})
			require.NoError(t, err)

			log3, err := ipfslog.NewLog(dag, identities[3], &ipfslog.LogOptions{ID: "X"})
			require.NoError(t, err)

			for i := 1; i <= 5; i++ {
				_, err := log1.Append(ctx, []byte(fmt.Sprintf("entryA%d", i)), nil)
				require.NoError(t, err)

				_, err = log2.Append(ctx, []byte(fmt.Sprintf("entryB%d", i)), nil)
				require.NoError(t, err)
			}

			_, err = log3.Join(log1, -1)
			require.NoError(t, err)

			_, err = log3.Join(log2, -1)
			require.NoError(t, err)

			for i := 6; i <= 10; i++ {
				_, err := log1.Append(ctx, []byte(fmt.Sprintf("entryA%d", i)), nil)
				require.NoError(t, err)
			}

			_, err = log1.Join(log3, -1)

			for i := 11; i <= 15; i++ {
				_, err := log1.Append(ctx, []byte(fmt.Sprintf("entryA%d", i)), nil)
				require.NoError(t, err)
			}

			expectedData := []string{"entryA1", "entryB1", "entryA2", "entryB2",
				"entryA3", "entryB3", "entryA4", "entryB4",
				"entryA5", "entryB5",
				"entryA6", "entryA7", "entryA8", "entryA9", "entryA10",
				"entryA11", "entryA12", "entryA13", "entryA14", "entryA15",
			}

			require.Equal(t, entriesAsStrings(log1.Values()), expectedData)
		})

		t.Run("retrieves randomly joined log deterministically", func(t *testing.T) {
			logA, err := ipfslog.NewLog(dag, identities[0], &ipfslog.LogOptions{ID: "X"})
			require.NoError(t, err)

			logB, err := ipfslog.NewLog(dag, identities[2], &ipfslog.LogOptions{ID: "X"})
			require.NoError(t, err)

			log3, err := ipfslog.NewLog(dag, identities[3], &ipfslog.LogOptions{ID: "X"})
			require.NoError(t, err)

			l, err := ipfslog.NewLog(dag, identities[1], &ipfslog.LogOptions{ID: "X"})
			require.NoError(t, err)

			for i := 1; i <= 5; i++ {
				_, err := logA.Append(ctx, []byte(fmt.Sprintf("entryA%d", i)), nil)
				require.NoError(t, err)

				_, err = logB.Append(ctx, []byte(fmt.Sprintf("entryB%d", i)), nil)
				require.NoError(t, err)
			}

			_, err = log3.Join(logA, -1)
			require.NoError(t, err)

			_, err = log3.Join(logB, -1)
			require.NoError(t, err)

			for i := 6; i <= 10; i++ {
				_, err := logA.Append(ctx, []byte(fmt.Sprintf("entryA%d", i)), nil)
				require.NoError(t, err)
			}

			_, err = l.Join(log3, -1)
			require.NoError(t, err)

			_, err = l.Append(ctx, []byte("entryC0"), nil)
			require.NoError(t, err)

			_, err = l.Join(logA, 16)
			require.NoError(t, err)

			expectedData := []string{
				"entryA1", "entryB1", "entryA2", "entryB2",
				"entryA3", "entryB3", "entryA4", "entryB4",
				"entryA5", "entryB5",
				"entryA6",
				"entryC0", "entryA7", "entryA8", "entryA9", "entryA10",
			}

			require.Equal(t, entriesAsStrings(l.Values()), expectedData)
		})

		t.Run("sorts", func(t *testing.T) {
			testLog, err := CreateLogWithSixteenEntries(ctx, dag, resortedIdentities)
			require.NoError(t, err)

			l := testLog.Log
			expectedData := testLog.ExpectedData

			expectedData2 := []string{
				"entryA1", "entryB1", "entryA2", "entryB2",
				"entryA3", "entryB3", "entryA4", "entryB4",
				"entryA5", "entryB5",
				"entryA6", "entryA7", "entryA8", "entryA9", "entryA10",
			}

			expectedData3 := []string{
				"entryA1", "entryB1", "entryA2", "entryB2",
				"entryA3", "entryB3", "entryA4", "entryB4",
				"entryA5", "entryB5", "entryA6", "entryC0",
				"entryA7", "entryA8", "entryA9",
			}

			expectedData4 := []string{
				"entryA1", "entryB1", "entryA2", "entryB2",
				"entryA3", "entryB3", "entryA4", "entryB4",
				"entryA5", "entryA6", "entryC0", "entryA7",
				"entryA8", "entryA9", "entryA10",
			}

			fetchOrder := l.Values().Slice()
			sorting.Sort(sorting.Compare, fetchOrder)
			require.Equal(t, entriesAsStrings(entry.NewOrderedMapFromEntries(fetchOrder)), expectedData)

			reverseOrder := l.Values().Slice()
			sorting.Reverse(reverseOrder)
			sorting.Sort(sorting.Compare, reverseOrder)
			require.Equal(t, entriesAsStrings(entry.NewOrderedMapFromEntries(reverseOrder)), expectedData)

			hashOrder := l.Values().Slice()
			sorting.Sort(func(a, b iface.IPFSLogEntry) (int, error) {
				return strings.Compare(a.GetHash().String(), b.GetHash().String()), nil
			}, hashOrder)
			sorting.Sort(sorting.Compare, hashOrder)
			require.Equal(t, entriesAsStrings(entry.NewOrderedMapFromEntries(hashOrder)), expectedData)

			var partialLog []iface.IPFSLogEntry
			for _, item := range l.Values().Slice() {
				if bytes.Compare(item.GetPayload(), []byte("entryC0")) != 0 {
					partialLog = append(partialLog, item)
				}
			}
			require.Equal(t, entriesAsStrings(entry.NewOrderedMapFromEntries(partialLog)), expectedData2)

			var partialLog2 []iface.IPFSLogEntry
			for _, item := range l.Values().Slice() {
				if bytes.Compare(item.GetPayload(), []byte("entryA10")) != 0 {
					partialLog2 = append(partialLog2, item)
				}
			}
			require.Equal(t, entriesAsStrings(entry.NewOrderedMapFromEntries(partialLog2)), expectedData3)

			var partialLog3 []iface.IPFSLogEntry
			for _, item := range l.Values().Slice() {
				if bytes.Compare(item.GetPayload(), []byte("entryB5")) != 0 {
					partialLog3 = append(partialLog3, item)
				}
			}
			require.Equal(t, entriesAsStrings(entry.NewOrderedMapFromEntries(partialLog3)), expectedData4)
		})

		t.Run("sorts deterministically from random order", func(t *testing.T) {
			testLog, err := CreateLogWithSixteenEntries(ctx, dag, resortedIdentities)
			require.NoError(t, err)

			l := testLog.Log
			expectedData := testLog.ExpectedData

			fetchOrder := l.Values().Slice()
			sorting.Sort(sorting.Compare, fetchOrder)
			require.Equal(t, entriesAsStrings(entry.NewOrderedMapFromEntries(fetchOrder)), expectedData)

			for i := 0; i < 1000; i++ {
				randomOrder := l.Values().Slice()
				sorting.Sort(func(a, b iface.IPFSLogEntry) (int, error) {
					return rand.Int(), nil
				}, randomOrder)
				sorting.Sort(sorting.Compare, randomOrder)

				require.Equal(t, entriesAsStrings(entry.NewOrderedMapFromEntries(randomOrder)), expectedData)
			}
		})

		t.Run("sorts entries correctly", func(t *testing.T) {
			testLog, err := CreateLogWithHundredEntries(ctx, dag, resortedIdentities)
			require.NoError(t, err)

			l := testLog.Log
			expectedData := testLog.ExpectedData

			require.Equal(t, entriesAsStrings(entry.NewOrderedMapFromEntries(l.Values().Slice())), expectedData)
		})

		t.Run("sorts entries according to custom tiebreaker function", func(t *testing.T) {
			testLog, err := CreateLogWithSixteenEntries(ctx, dag, resortedIdentities)
			require.NoError(t, err)

			firstWriteWinsLog, err := ipfslog.NewLog(dag, resortedIdentities[0], &ipfslog.LogOptions{ID: "X", SortFn: BadComparatorReturnsZero})
			require.NoError(t, err)

			_, err = firstWriteWinsLog.Join(testLog.Log, -1)
			// TODO: the error is only thrown silently when calling .Values(), should we handle it properly
			//firstWriteWinsLog.Values()
			//require.NotNil(t, err)
		})

		t.Run("retrieves partially joined log deterministically - single next pointer", func(t *testing.T) {
			nextPointersAmount := 1

			logA, err := ipfslog.NewLog(dag, identities[0], &ipfslog.LogOptions{ID: "X"})
			require.NoError(t, err)
			logB, err := ipfslog.NewLog(dag, identities[2], &ipfslog.LogOptions{ID: "X"})
			require.NoError(t, err)
			log3, err := ipfslog.NewLog(dag, identities[3], &ipfslog.LogOptions{ID: "X"})
			require.NoError(t, err)
			l, err := ipfslog.NewLog(dag, identities[1], &ipfslog.LogOptions{ID: "X"})
			require.NoError(t, err)

			for i := 1; i <= 5; i++ {
				_, err := logA.Append(ctx, []byte(fmt.Sprintf("entryA%d", i)), &iface.AppendOptions{
					PointerCount: nextPointersAmount,
				})
				require.NoError(t, err)

				_, err = logB.Append(ctx, []byte(fmt.Sprintf("entryB%d", i)), &iface.AppendOptions{
					PointerCount: nextPointersAmount,
				})
				require.NoError(t, err)
			}

			_, err = log3.Join(logA, -1)
			require.NoError(t, err)

			_, err = log3.Join(logB, -1)
			require.NoError(t, err)

			for i := 6; i <= 10; i++ {
				_, err = logA.Append(ctx, []byte(fmt.Sprintf("entryA%d", i)), &iface.AppendOptions{
					PointerCount: nextPointersAmount,
				})
				require.NoError(t, err)
			}

			_, err = l.Join(log3, -1)
			require.NoError(t, err)

			_, err = l.Append(ctx, []byte("entryC0"), &iface.AppendOptions{
				PointerCount: nextPointersAmount,
			})
			require.NoError(t, err)

			_, err = l.Join(logA, -1)
			require.NoError(t, err)

			hash, err := l.ToMultihash(ctx)
			require.NoError(t, err)

			// First 5
			res, err := ipfslog.NewFromMultihash(ctx, dag, identities[1], hash, &ipfslog.LogOptions{}, &ipfslog.FetchOptions{Length: intPtr(5)})
			require.NoError(t, err)

			first5 := []string{
				"entryC0", "entryA7", "entryA8", "entryA9", "entryA10",
			}

			require.Equal(t, entriesAsStrings(res.Values()), first5)

			// First 11
			res, err = ipfslog.NewFromMultihash(ctx, dag, identities[1], hash, &ipfslog.LogOptions{}, &ipfslog.FetchOptions{Length: intPtr(11)})
			require.NoError(t, err)

			first11 := []string{
				"entryB3", "entryA4", "entryB4",
				"entryA5", "entryB5",
				"entryA6",
				"entryC0", "entryA7", "entryA8", "entryA9", "entryA10",
			}

			require.Equal(t, entriesAsStrings(res.Values()), first11)

			// All but one
			res, err = ipfslog.NewFromMultihash(ctx, dag, identities[1], hash, &ipfslog.LogOptions{}, &ipfslog.FetchOptions{Length: intPtr(16 - 1)})
			require.NoError(t, err)

			all := []string{
				/* excl */ "entryB1", "entryA2", "entryB2", "entryA3", "entryB3",
				"entryA4", "entryB4", "entryA5", "entryB5",
				"entryA6",
				"entryC0", "entryA7", "entryA8", "entryA9", "entryA10",
			}

			require.Equal(t, entriesAsStrings(res.Values()), all)

		})

		t.Run("retrieves partially joined log deterministically - multiple next pointers", func(t *testing.T) {
			nextPointersAmount := 64

			logA, err := ipfslog.NewLog(dag, identities[0], &ipfslog.LogOptions{ID: "X"})
			require.NoError(t, err)
			logB, err := ipfslog.NewLog(dag, identities[2], &ipfslog.LogOptions{ID: "X"})
			require.NoError(t, err)
			log3, err := ipfslog.NewLog(dag, identities[3], &ipfslog.LogOptions{ID: "X"})
			require.NoError(t, err)
			l, err := ipfslog.NewLog(dag, identities[1], &ipfslog.LogOptions{ID: "X"})
			require.NoError(t, err)

			for i := 1; i <= 5; i++ {
				_, err = logA.Append(ctx, []byte(fmt.Sprintf("entryA%d", i)), &iface.AppendOptions{
					PointerCount: nextPointersAmount,
				})
				require.NoError(t, err)
			}

			for i := 1; i <= 5; i++ {
				_, err = logB.Append(ctx, []byte(fmt.Sprintf("entryB%d", i)), &iface.AppendOptions{
					PointerCount: nextPointersAmount,
				})
				require.NoError(t, err)
			}

			_, err = log3.Join(logA, -1)
			require.NoError(t, err)

			_, err = log3.Join(logB, -1)
			require.NoError(t, err)

			for i := 6; i <= 10; i++ {
				_, err = logA.Append(ctx, []byte(fmt.Sprintf("entryA%d", i)), &iface.AppendOptions{
					PointerCount: nextPointersAmount,
				})
				require.NoError(t, err)
			}

			_, err = l.Join(log3, -1)
			require.NoError(t, err)

			_, err = l.Append(ctx, []byte("entryC0"), &iface.AppendOptions{
				PointerCount: nextPointersAmount,
			})
			require.NoError(t, err)

			_, err = l.Join(logA, -1)
			require.NoError(t, err)

			hash, err := l.ToMultihash(ctx)

			// First 5
			res, err := ipfslog.NewFromMultihash(ctx, dag, identities[1], hash, &ipfslog.LogOptions{}, &ipfslog.FetchOptions{Length: intPtr(5)})
			require.NoError(t, err)

			first5 := []string{
				"entryC0", "entryA7", "entryA8", "entryA9", "entryA10",
			}

			require.Equal(t, entriesAsStrings(res.Values()), first5)

			// First 11
			res, err = ipfslog.NewFromMultihash(ctx, dag, identities[1], hash, &ipfslog.LogOptions{}, &ipfslog.FetchOptions{Length: intPtr(11)})
			require.NoError(t, err)

			first11 := []string{
				"entryB3", "entryA4", "entryB4", "entryA5",
				"entryB5", "entryA6",
				"entryC0",
				"entryA7", "entryA8", "entryA9", "entryA10",
			}

			require.Equal(t, entriesAsStrings(res.Values()), first11)

			// All but one
			res, err = ipfslog.NewFromMultihash(ctx, dag, identities[1], hash, &ipfslog.LogOptions{}, &ipfslog.FetchOptions{Length: intPtr(16 - 1)})
			require.NoError(t, err)

			all := []string{
				/* excl */ "entryB1", "entryA2", "entryB2", "entryA3", "entryB3",
				"entryA4", "entryB4", "entryA5", "entryB5",
				"entryA6",
				"entryC0", "entryA7", "entryA8", "entryA9", "entryA10",
			}

			require.Equal(t, entriesAsStrings(res.Values()), all)
		})

		t.Run("throws an error if ipfs is not defined", func(t *testing.T) {
			_, err := ipfslog.NewFromEntry(ctx, nil, identities[0], []iface.IPFSLogEntry{}, &ipfslog.LogOptions{ID: "X"}, &entry.FetchOptions{})
			require.Error(t, err)
			require.Contains(t, err.Error(), errmsg.ErrIPFSNotDefined.Error())
		})

		t.Run("fetches a log", func(t *testing.T) {
			const amount = 100

			ts := time.Now().UnixNano() / 1000
			log1, err := ipfslog.NewLog(dag, identities[0], &ipfslog.LogOptions{ID: "X"})
			require.NoError(t, err)

			log2, err := ipfslog.NewLog(dag, identities[1], &ipfslog.LogOptions{ID: "X"})
			require.NoError(t, err)

			log3, err := ipfslog.NewLog(dag, identities[2], &ipfslog.LogOptions{ID: "X"})
			require.NoError(t, err)

			var items1 []iface.IPFSLogEntry
			var items2 []iface.IPFSLogEntry
			var items3 []iface.IPFSLogEntry

			for i := 1; i <= amount; i++ {
				var nexts []cid.Cid
				prev1 := lastEntry(items1)
				prev2 := lastEntry(items2)
				prev3 := lastEntry(items3)

				if prev1 != nil {
					nexts = []cid.Cid{prev1.GetHash()}
				}

				n1, err := entry.CreateEntry(ctx, dag, log1.Identity, &entry.Entry{LogID: log1.ID, Payload: []byte(fmt.Sprintf("entryA%d-%d", i, ts)), Next: nexts, Clock: entry.CopyLamportClock(log1.Clock)}, nil)
				require.NoError(t, err)

				nexts = []cid.Cid{n1.GetHash()}
				if prev2 != nil {
					nexts = []cid.Cid{prev2.GetHash(), n1.GetHash()}
				}

				n2, err := entry.CreateEntry(ctx, dag, log2.Identity, &entry.Entry{LogID: log2.ID, Payload: []byte(fmt.Sprintf("entryB%d-%d", i, ts)), Next: nexts, Clock: entry.CopyLamportClock(log2.Clock)}, nil)
				require.NoError(t, err)

				nexts = []cid.Cid{n1.GetHash(), n2.GetHash()}
				if prev2 != nil {
					nexts = []cid.Cid{prev3.GetHash(), n1.GetHash(), n2.GetHash()}
				}

				n3, err := entry.CreateEntry(ctx, dag, log3.Identity, &entry.Entry{LogID: log3.ID, Payload: []byte(fmt.Sprintf("entryC%d-%d", i, ts)), Next: nexts, Clock: entry.CopyLamportClock(log3.Clock)}, nil)
				require.NoError(t, err)

				log1.Clock.Tick()
				log2.Clock.Tick()
				log3.Clock.Tick()
				log1.Clock.Merge(log2.Clock)
				log1.Clock.Merge(log3.Clock)
				log2.Clock.Merge(log1.Clock)
				log2.Clock.Merge(log3.Clock)
				log3.Clock.Merge(log1.Clock)
				log3.Clock.Merge(log2.Clock)
				items1 = append(items1, n1)
				items2 = append(items2, n2)
				items3 = append(items3, n3)
			}

			t.Run("returns all entries - no excluded entries", func(t *testing.T) {
				a, err := ipfslog.NewFromEntry(ctx, dag, identities[0], []iface.IPFSLogEntry{lastEntry(items1)}, &ipfslog.LogOptions{}, &entry.FetchOptions{Length: intPtr(-1)})
				require.NoError(t, err)

				require.Equal(t, a.Values().Len(), amount)
				require.Equal(t, a.Values().At(0).GetHash().String(), items1[0].GetHash().String())
			})

			t.Run("returns all entries - including excluded entries", func(t *testing.T) {
				// One entry
				a, err := ipfslog.NewFromEntry(ctx, dag, identities[0], []iface.IPFSLogEntry{lastEntry(items1)}, &ipfslog.LogOptions{}, &entry.FetchOptions{Exclude: []iface.IPFSLogEntry{items1[0]}, Length: intPtr(-1)})
				require.NoError(t, err)

				require.Equal(t, a.Values().Len(), amount)
				require.Equal(t, a.Values().At(0).GetHash().String(), items1[0].GetHash().String())

				// All entries
				b, err := ipfslog.NewFromEntry(ctx, dag, identities[0], []iface.IPFSLogEntry{lastEntry(items1)}, &ipfslog.LogOptions{}, &entry.FetchOptions{Exclude: items1, Length: intPtr(-1)})
				require.NoError(t, err)

				require.Equal(t, b.Values().Len(), amount)
				require.Equal(t, b.Values().At(0).GetHash().String(), items1[0].GetHash().String())
			})
		})

		t.Run("respects timeout parameter", func(t *testing.T) {
			// TODO
		})
	})

	t.Run("Backwards-compatibility v0", func(t *testing.T) {
		v0Entries := getEntriesV0Fixtures(t)
		pbio, err := pb.IO(&entry.Entry{}, &entry.LamportClock{})
		require.NoError(t, err)

		_, err = pbio.Write(ctx, dag, nil, entry.Normalize(v0Entries["hello"], nil))
		require.NoError(t, err)

		c, err := pbio.Write(ctx, dag, nil, entry.Normalize(v0Entries["helloWorld"], nil))
		require.NoError(t, err)

		require.Equal(t, "QmUKMoRrmsYAzQg1nQiD7Fzgpo24zXky7jVJNcZGiSAdhc", c.String())

		c, err = pbio.Write(ctx, dag, nil, entry.Normalize(v0Entries["helloAgain"], nil))
		require.NoError(t, err)

		require.Equal(t, "QmZ8va2fSjRufV1sD6x5mwi6E5GrSjXHx7RiKFVBzkiUNZ", c.String())
		testIdentity := identities[0]

		t.Run("creates a log from v0 json", func(t *testing.T) {
			headHash, err := pbio.Write(ctx, dag, nil, entry.Normalize(v0Entries["helloAgain"], nil))
			require.NoError(t, err)

			json := &iface.JSONLog{
				ID:    "A",
				Heads: []cid.Cid{headHash},
			}

			headEntries := []iface.IPFSLogEntry(nil)

			for _, head := range json.Heads {
				e, err := entry.FromMultihashWithIO(ctx, dag, head, testIdentity.Provider, pbio)
				require.NoError(t, err)

				headEntries = append(headEntries, e)
			}

			l, err := ipfslog.NewFromJSON(ctx, dag, testIdentity, json, &ipfslog.LogOptions{ID: "A", IO: pbio}, &entry.FetchOptions{Length: intPtr(-1), IO: pbio})
			require.NoError(t, err)

			require.Equal(t, 2, l.Values().Len())
		})

		t.Run("creates a log from v0 entry", func(t *testing.T) {
			log, err := ipfslog.NewFromEntry(ctx, dag, testIdentity, []iface.IPFSLogEntry{v0Entries["helloAgain"]},
				&ipfslog.LogOptions{
					ID: "A",
					IO: pbio,
				}, &entry.FetchOptions{IO: pbio})
			require.NoError(t, err)

			require.Equal(t, 2, log.Entries.Len())
		})

		t.Run("creates a log from v0 entry hash", func(t *testing.T) {
			log, err := ipfslog.NewFromEntryHash(ctx, dag, testIdentity, v0Entries["helloAgain"].Hash,
				&ipfslog.LogOptions{
					ID: "A",
					IO: pbio,
				}, &ipfslog.FetchOptions{})
			require.NoError(t, err)

			require.Equal(t, 2, log.Entries.Len())
		})

		t.Run("creates a log from v0 entry", func(t *testing.T) {
			log, err := ipfslog.NewFromEntry(ctx, dag, testIdentity, []iface.IPFSLogEntry{v0Entries["helloAgain"]},
				&ipfslog.LogOptions{
					ID: "A",
					IO: pbio,
				}, &entry.FetchOptions{})
			require.NoError(t, err)

			require.Equal(t, 2, log.Entries.Len())
		})
	})
}
