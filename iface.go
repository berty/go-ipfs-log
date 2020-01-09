package ipfslog

import (
	"context"
	"time"

	"berty.tech/go-ipfs-log/accesscontroller"
	"berty.tech/go-ipfs-log/identity"
	"github.com/iancoleman/orderedmap"
	"github.com/ipfs/go-cid"
)

type Log interface {
	GetID() string
	Append(ctx context.Context, payload []byte, pointerCount int) (Entry, error)
	Iterator(options *IteratorOptions, output chan<- Entry) error
	Join(otherLog Log, size int) (Log, error)
	ToString(payloadMapper func(Entry) string) string
	ToSnapshot() *Snapshot
	ToMultihash(ctx context.Context) (cid.Cid, error)
	Values() OrderedEntries
	ToJSON() *JSONLog
	Heads() OrderedEntries
	GetEntries() OrderedEntries
	SetEntries(OrderedEntries)
	RawHeads() OrderedEntries
}

type FetchOptions struct {
	Length       *int
	Exclude      []Entry
	Concurrency  int
	Timeout      time.Duration
	ProgressChan chan Entry
	Provider     identity.Provider
}

type LogOptions struct {
	ID               string
	AccessController accesscontroller.Interface
	Entries          OrderedEntries
	Heads            []Entry
	Clock            LamportClock
	SortFn           func(a, b Entry) (int, error)
}

type JSONLog struct {
	ID    string
	Heads []cid.Cid
}

type IteratorOptions struct {
	GT     *cid.Cid
	GTE    *cid.Cid
	LT     []cid.Cid
	LTE    []cid.Cid
	Amount *int
}

type Snapshot struct {
	ID     string
	Heads  []cid.Cid
	Values []Entry
	Clock  LamportClock
}

type OrderedEntries interface {
	// Merge will fusion two OrderedMap of entries.
	Merge(other OrderedEntries) OrderedEntries

	// Copy creates a copy of an OrderedMap.
	Copy() OrderedEntries

	// Get retrieves an Entry using its key.
	Get(key string) (Entry, bool)

	// UnsafeGet retrieves an Entry using its key, returns nil if not found.
	UnsafeGet(key string) Entry

	// Set defines an Entry in the map for a given key.
	Set(key string, value Entry)

	// Slice returns an ordered slice of the values existing in the map.
	Slice() []Entry

	// Delete removes an Entry from the map for a given key.
	Delete(key string)

	// Keys retrieves the ordered list of keys in the map.
	Keys() []string

	// SortKeys orders the map keys using your sort func.
	SortKeys(sortFunc func(keys []string))

	// Sort orders the map using your sort func.
	Sort(lessFunc func(a *orderedmap.Pair, b *orderedmap.Pair) bool)

	// Len gets the length of the map.
	Len() int

	// At gets an item at the given index in the map, returns nil if not found.
	At(index uint) Entry
}

type Entry interface {
	accesscontroller.LogEntry

	GetLogID() string
	GetNext() []cid.Cid
	GetV() uint64
	GetKey() []byte
	GetSig() []byte
	GetHash() cid.Cid
	GetClock() LamportClock

	SetPayload([]byte)
	SetLogID(string)
	SetNext([]cid.Cid)
	SetV(uint64)
	SetKey([]byte)
	SetSig([]byte)
	SetIdentity(*identity.Identity)
	SetHash(cid.Cid)
	SetClock(LamportClock)

	IsValid() bool
	Verify(identity identity.Provider) error
	IsParent(b Entry) bool
	ToCborEntry() interface{}
}

type LamportClock interface {
	GetID() []byte
	GetTime() int

	Tick() LamportClock
	Merge(clock LamportClock) LamportClock
	Compare(b LamportClock) int

	ToCborLamportClock() *CborLamportClock
}
