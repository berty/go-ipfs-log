package iface

import (
	"context"
	"time"

	"berty.tech/go-ipfs-log/accesscontroller"
	"berty.tech/go-ipfs-log/identityprovider"
	"github.com/iancoleman/orderedmap"
	"github.com/ipfs/go-cid"
)

type CborLamportClock struct {
	ID   string
	Time int
}

type FetchOptions struct {
	Length       *int
	Exclude      []IPFSLogEntry
	Concurrency  int
	Timeout      time.Duration
	ProgressChan chan IPFSLogEntry
	Provider     identityprovider.Interface
}

type LogOptions struct {
	ID               string
	AccessController accesscontroller.Interface
	Entries          IPFSLogOrderedEntries
	Heads            []IPFSLogEntry
	Clock            IPFSLogLamportClock
	SortFn           func(a, b IPFSLogEntry) (int, error)
	Concurrency      uint
}

type CreateEntryOptions struct {
	Pin       bool
	PreSigned bool
}

type JSONLog struct {
	ID    string
	Heads []cid.Cid
}

type IteratorOptions struct {
	GT     cid.Cid
	GTE    cid.Cid
	LT     []cid.Cid
	LTE    []cid.Cid
	Amount *int
}

type Snapshot struct {
	ID     string
	Heads  []cid.Cid
	Values []IPFSLogEntry
	Clock  IPFSLogLamportClock
}

type AppendOptions struct {
	PointerCount int
	Pin          bool
}

type IPFSLog interface {
	GetID() string
	Append(ctx context.Context, payload []byte, opts *AppendOptions) (IPFSLogEntry, error)
	Iterator(options *IteratorOptions, output chan<- IPFSLogEntry) error
	Join(otherLog IPFSLog, size int) (IPFSLog, error)
	ToString(payloadMapper func(IPFSLogEntry) string) string
	ToSnapshot() *Snapshot
	ToMultihash(ctx context.Context) (cid.Cid, error)
	Values() IPFSLogOrderedEntries
	ToJSON() *JSONLog
	Heads() IPFSLogOrderedEntries
	GetEntries() IPFSLogOrderedEntries
	SetEntries(IPFSLogOrderedEntries)
	RawHeads() IPFSLogOrderedEntries
	SetIdentity(identity *identityprovider.Identity)
}

type EntrySortFn func(IPFSLogEntry, IPFSLogEntry) (int, error)

type IPFSLogOrderedEntries interface {
	// Merge will fusion two OrderedMap of entries.
	Merge(other IPFSLogOrderedEntries) IPFSLogOrderedEntries

	// Copy creates a copy of an OrderedMap.
	Copy() IPFSLogOrderedEntries

	// Get retrieves an Entry using its key.
	Get(key string) (IPFSLogEntry, bool)

	// UnsafeGet retrieves an Entry using its key, returns nil if not found.
	UnsafeGet(key string) IPFSLogEntry

	// Set defines an Entry in the map for a given key.
	Set(key string, value IPFSLogEntry)

	// Slice returns an ordered slice of the values existing in the map.
	Slice() []IPFSLogEntry

	// First
	First(until uint) IPFSLogOrderedEntries

	// Last
	Last(after uint) IPFSLogOrderedEntries

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
	At(index uint) IPFSLogEntry

	Reverse() IPFSLogOrderedEntries
}

type IPFSLogEntry interface {
	accesscontroller.LogEntry

	GetLogID() string
	GetNext() []cid.Cid
	GetRefs() []cid.Cid
	GetV() uint64
	GetKey() []byte
	GetSig() []byte
	GetHash() cid.Cid
	GetClock() IPFSLogLamportClock

	SetPayload([]byte)
	SetLogID(string)
	SetNext([]cid.Cid)
	SetRefs([]cid.Cid)
	SetV(uint64)
	SetKey([]byte)
	SetSig([]byte)
	SetIdentity(*identityprovider.Identity)
	SetHash(cid.Cid)
	SetClock(IPFSLogLamportClock)

	IsValid() bool
	Verify(identity identityprovider.Interface) error
	IsParent(b IPFSLogEntry) bool
	ToCborEntry() interface{}
}

type IPFSLogLamportClock interface {
	GetID() []byte
	GetTime() int

	Tick() IPFSLogLamportClock
	Merge(clock IPFSLogLamportClock) IPFSLogLamportClock
	Compare(b IPFSLogLamportClock) int

	ToCborLamportClock() *CborLamportClock
}
