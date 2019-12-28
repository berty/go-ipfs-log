// Package ipfslog implements an append-only log CRDT on IPFS
package ipfslog // import "berty.tech/go-ipfs-log"

import (
	"berty.tech/go-ipfs-log/iface"
	"context"
	"strconv"
	"strings"
	"sync"
	"time"

	"berty.tech/go-ipfs-log/entry/sorting"

	"berty.tech/go-ipfs-log/accesscontroller"
	"berty.tech/go-ipfs-log/entry"
	"berty.tech/go-ipfs-log/errmsg"
	"berty.tech/go-ipfs-log/identityprovider"
	"berty.tech/go-ipfs-log/io"
	"github.com/iancoleman/orderedmap"
	"github.com/ipfs/go-cid"
	cbornode "github.com/ipfs/go-ipld-cbor"
	"github.com/pkg/errors"
	"github.com/polydawn/refmt/obj/atlas"
)

type Snapshot = iface.Snapshot
type JSONLog = iface.JSONLog
type LogOptions = iface.LogOptions
type IteratorOptions = iface.IteratorOptions

type Entry = iface.IPFSLogEntry
type Log = iface.IPFSLog

type IPFSLog struct {
	Storage          io.IpfsServices
	ID               string
	AccessController accesscontroller.Interface
	SortFn           func(a iface.IPFSLogEntry, b iface.IPFSLogEntry) (int, error)
	Identity         *identityprovider.Identity
	Entries          iface.IPFSLogOrderedEntries
	heads            iface.IPFSLogOrderedEntries
	Next             iface.IPFSLogOrderedEntries
	Clock            iface.IPFSLogLamportClock
	lock             sync.RWMutex
}

func (l *IPFSLog) GetClock() iface.IPFSLogLamportClock {
	l.lock.RLock()
	defer l.lock.RUnlock()

	return l.Clock
}

func (l *IPFSLog) SetEntries(entries iface.IPFSLogOrderedEntries) {
	l.lock.Lock()
	defer l.lock.Unlock()
	l.Entries = entries
}

func (l *IPFSLog) RawHeads() iface.IPFSLogOrderedEntries {
	l.lock.RLock()
	heads := l.heads
	l.lock.RUnlock()

	return heads
}

// maxInt Returns the larger of x or y
func maxInt(x, y int) int {
	if x < y {
		return y
	}
	return x
}

func maxClockTimeForEntries(entries []iface.IPFSLogEntry, defValue int) int {
	max := defValue
	for _, e := range entries {
		max = maxInt(e.GetClock().GetTime(), max)
	}

	return max
}

// NewLog Creates creates a new IPFSLog for a given identity
//
// Each IPFSLog gets a unique ID, which can be passed in the options as ID.
//
// Returns a log instance.
//
// ipfs is an instance of IPFS.
//
// identity is an instance of Identity and will be used to sign entries
// Usually this should be a user id or similar.
//
// options.AccessController is an instance of accesscontroller.Interface,
// which by default allows anyone to append to the IPFSLog.
func NewLog(services io.IpfsServices, identity *identityprovider.Identity, options *LogOptions) (*IPFSLog, error) {
	if services == nil {
		return nil, errmsg.IPFSNotDefined
	}

	if identity == nil {
		return nil, errmsg.IdentityNotDefined
	}

	if options == nil {
		options = &LogOptions{}
	}

	if options.ID == "" {
		options.ID = strconv.FormatInt(time.Now().Unix()/1000, 10)
	}

	if options.SortFn == nil {
		options.SortFn = sorting.LastWriteWins
	}

	maxTime := 0
	if options.Clock != nil {
		maxTime = options.Clock.GetTime()
	}
	maxTime = maxClockTimeForEntries(options.Heads, maxTime)

	if options.AccessController == nil {
		options.AccessController = &accesscontroller.Default{}
	}

	if options.Entries == nil {
		options.Entries = entry.NewOrderedMap()
	}

	if len(options.Heads) == 0 && len(options.Entries.Keys()) > 0 {
		options.Heads = entry.FindHeads(options.Entries)
	}

	next := entry.NewOrderedMap()
	for _, key := range options.Entries.Keys() {
		entry := options.Entries.UnsafeGet(key)
		for _, n := range entry.GetNext() {
			next.Set(n.String(), entry)
		}
	}

	return &IPFSLog{
		Storage:          services,
		ID:               options.ID,
		Identity:         identity,
		AccessController: options.AccessController,
		SortFn:           sorting.NoZeroes(options.SortFn),
		Entries:          options.Entries.Copy(),
		heads:            entry.NewOrderedMapFromEntries(options.Heads),
		Next:             next,
		Clock:            entry.NewLamportClock(identity.PublicKey, maxTime),
	}, nil
}

// addToStack Add an entry to the stack and traversed nodes index
func (l *IPFSLog) addToStack(e iface.IPFSLogEntry, stack []iface.IPFSLogEntry, traversed *orderedmap.OrderedMap) ([]iface.IPFSLogEntry, *orderedmap.OrderedMap) {
	// If we've already processed the entry, don't add it to the stack
	if _, ok := traversed.Get(e.GetHash().String()); ok {
		return stack, traversed
	}

	// Add the entry in front of the stack and sort
	stack = append([]iface.IPFSLogEntry{e}, stack...)
	sorting.Sort(l.SortFn, stack)
	sorting.Reverse(stack)

	// Add to the cache of processed entries
	traversed.Set(e.GetHash().String(), true)

	return stack, traversed
}

func (l *IPFSLog) traverse(rootEntries iface.IPFSLogOrderedEntries, amount int, endHash string) ([]iface.IPFSLogEntry, error) {
	if rootEntries == nil {
		return nil, errmsg.EntriesNotDefined
	}

	// Sort the given given root entries and use as the starting stack
	stack := rootEntries.Slice()

	sorting.Sort(l.SortFn, stack)
	sorting.Reverse(stack)

	// Cache for checking if we've processed an entry already
	traversed := orderedmap.New()
	// End result
	var result []iface.IPFSLogEntry
	// We keep a counter to check if we have traversed requested amount of entries
	count := 0

	// Start traversal
	// Process stack until it's empty (traversed the full log)
	// or when we have the requested amount of entries
	// If requested entry amount is -1, traverse all
	for len(stack) > 0 && (amount < 0 || count < amount) {
		// Get the next element from the stack
		e := stack[0]
		stack = stack[1:]

		// Add to the result
		count++
		result = append(result, e)

		// Add entry's next references to the stack
		for _, next := range e.GetNext() {
			nextEntry, ok := l.Entries.Get(next.String())
			if !ok {
				continue
			}

			stack, traversed = l.addToStack(nextEntry, stack, traversed)
		}

		// If it is the specified end hash, break out of the while loop
		if e.GetHash().String() == endHash {
			break
		}
	}

	return result, nil
}

// Append Appends an entry to the log Returns the latest Entry
//
// payload is the data that will be in the Entry
func (l *IPFSLog) Append(ctx context.Context, payload []byte, pointerCount int) (iface.IPFSLogEntry, error) {
	// INFO: JS default value for pointerCount is 1
	// Update the clock (find the latest clock)
	l.lock.RLock()
	heads := l.heads
	l.lock.RUnlock()

	newTime := maxClockTimeForEntries(heads.Slice(), 0)
	newTime = maxInt(l.GetClock().GetTime(), newTime) + 1

	clockID := l.GetClock().GetID()

	l.lock.Lock()
	l.Clock = entry.NewLamportClock(clockID, newTime)
	l.lock.Unlock()

	// Get the required amount of hashes to next entries (as per current state of the log)
	references, err := l.traverse(heads, maxInt(pointerCount, heads.Len()), "")
	if err != nil {
		return nil, errors.Wrap(err, "append failed")
	}

	next := []cid.Cid{}

	keys := heads.Keys()
	for _, k := range keys {
		e, _ := heads.Get(k)
		next = append(next, e.GetHash())
	}
	for _, e := range references {
		next = append(next, e.GetHash())
	}

	// TODO: ensure port of ```Object.keys(Object.assign({}, this._headsIndex, references))``` is correctly implemented

	// @TODO: Split Entry.create into creating object, checking permission, signing and then posting to IPFS
	// Create the entry and add it to the internal cache
	e, err := entry.CreateEntry(ctx, l.Storage, l.Identity, &entry.Entry{
		LogID:   l.ID,
		Payload: payload,
		Next:    next,
	}, l.GetClock())
	if err != nil {
		return nil, errors.Wrap(err, "append failed")
	}

	if err := l.AccessController.CanAppend(e, l.Identity.Provider, &CanAppendContext{log: l}); err != nil {
		return nil, errors.Wrap(err, "append failed")
	}

	l.Entries.Set(e.Hash.String(), e)

	for _, k := range keys {
		nextEntry, _ := heads.Get(k)
		l.Next.Set(nextEntry.GetHash().String(), e)
	}

	heads = entry.NewOrderedMapFromEntries([]iface.IPFSLogEntry{e})

	l.lock.Lock()
	l.heads = heads
	l.lock.Unlock()

	return e, nil
}

type CanAppendContext struct {
	log *IPFSLog
}

func (c *CanAppendContext) GetLogEntries() []accesscontroller.LogEntry {
	logEntries := c.log.Entries.Slice()

	var entries = make([]accesscontroller.LogEntry, len(logEntries))
	for i := range logEntries {
		entries[i] = logEntries[i]
	}

	return entries
}

/* Iterator Provides entries values on a channel */
func (l *IPFSLog) Iterator(options *IteratorOptions, output chan<- iface.IPFSLogEntry) error {
	amount := -1
	if options == nil {
		return errors.New("no options specified")
	}

	if output == nil {
		return errors.New("no output channel specified")
	}

	if options.Amount != nil {
		if *options.Amount == 0 {
			return nil
		}
		amount = *options.Amount
	}

	l.lock.RLock()
	start := l.heads.Slice()
	l.lock.RUnlock()

	if options.LTE != nil {
		start = nil

		for _, c := range options.LTE {
			e, ok := l.Values().Get(c.String())
			if !ok {
				return errors.New("entry specified at LTE not found")
			}
			start = append(start, e)
		}
	} else if options.LT != nil {
		values := l.Values()

		for _, c := range options.LT {
			e, ok := values.Get(c.String())
			if !ok {
				return errors.New("entry specified at LT not found")
			}

			start = nil
			for _, n := range e.GetNext() {
				e, ok := values.Get(n.String())
				if !ok {
					return errors.New("entry specified at LT not found")
				}
				start = append(start, e)
			}
		}
	}

	endHash := ""
	if options.GTE != nil {
		endHash = options.GTE.String()
	} else if options.GT != nil {
		endHash = options.GT.String()
	}

	count := -1
	if endHash == "" && options.Amount != nil {
		count = amount
	}

	entries, err := l.traverse(entry.NewOrderedMapFromEntries(start), count, endHash)
	if err != nil {
		return errors.Wrap(err, "iterator failed")
	}

	if options.GT != nil {
		entries = entries[:len(entries)-1]
	}

	// Deal with the amount argument working backwards from gt/gte
	if (options.GT != nil || options.GTE != nil) && amount > -1 {
		entries = entries[len(entries)-amount:]
	}

	for i := range entries {
		output <- entries[i]
	}

	close(output)

	return nil
}

// Join Joins the log with another log
//
// Returns a log instance.
//
// The size of the joined log can be specified by specifying the size argument, to include all values use -1
func (l *IPFSLog) Join(otherLog iface.IPFSLog, size int) (iface.IPFSLog, error) {
	// INFO: JS default size is -1
	if otherLog == nil {
		return nil, errmsg.LogJoinNotDefined
	}

	if l.ID != otherLog.GetID() {
		return l, nil
	}

	newItems := difference(otherLog, l)

	for _, k := range newItems.Keys() {
		e := newItems.UnsafeGet(k)
		if err := l.AccessController.CanAppend(e, l.Identity.Provider, &CanAppendContext{log: l}); err != nil {
			return nil, errors.Wrap(err, "join failed")
		}

		if err := e.Verify(l.Identity.Provider); err != nil {
			return nil, errors.Wrap(err, "unable to check signature")
		}
	}

	for _, k := range newItems.Keys() {
		e := newItems.UnsafeGet(k)
		for _, next := range e.GetNext() {
			l.Next.Set(next.String(), e)
		}

		l.Entries.Set(e.GetHash().String(), e)
	}

	nextsFromNewItems := orderedmap.New()
	for _, k := range newItems.Keys() {
		e := newItems.UnsafeGet(k)
		for _, n := range e.GetNext() {
			nextsFromNewItems.Set(n.String(), true)
		}
	}

	l.lock.RLock()
	heads := l.heads
	l.lock.RUnlock()

	mergedHeads := entry.FindHeads(heads.Merge(otherLog.RawHeads()))

	for idx, e := range mergedHeads {
		// notReferencedByNewItems
		if _, ok := nextsFromNewItems.Get(e.GetHash().String()); ok {
			mergedHeads[idx] = nil
		}

		// notInCurrentNexts
		if _, ok := l.Next.Get(e.GetHash().String()); ok {
			mergedHeads[idx] = nil
		}
	}

	l.lock.Lock()
	l.heads = entry.NewOrderedMapFromEntries(mergedHeads)
	l.lock.Unlock()

	if size > -1 {
		tmp := l.Values().Slice()
		tmp = tmp[len(tmp)-size:]

		entries := entry.NewOrderedMapFromEntries(tmp)
		heads := entry.NewOrderedMapFromEntries(entry.FindHeads(entry.NewOrderedMapFromEntries(tmp)))

		l.lock.Lock()
		l.Entries = entries
		l.heads = heads
		l.lock.Unlock()
	}

	// Find the latest clock from the heads
	l.lock.RLock()
	headsSlice := l.heads.Slice()
	clockID := l.GetClock().GetID()

	maxClock := maxClockTimeForEntries(headsSlice, 0)
	clockTime := maxInt(l.GetClock().GetTime(), maxClock)
	l.lock.RUnlock()

	l.lock.Lock()
	l.Clock = entry.NewLamportClock(clockID, clockTime)
	l.lock.Unlock()

	return l, nil
}

func difference(logA, logB iface.IPFSLog) iface.IPFSLogOrderedEntries {
	if logA == nil || logA.GetEntries() == nil || logA.GetEntries().Len() == 0 || logB == nil {
		return entry.NewOrderedMap()
	}

	if logB.GetEntries() == nil {
		logB.SetEntries(entry.NewOrderedMap())
	}

	stack := logA.RawHeads().Keys()
	traversed := map[string]bool{}
	res := entry.NewOrderedMap()

	for {
		if len(stack) == 0 {
			break
		}
		hash := stack[0]
		stack = stack[1:]

		eA, okA := logA.GetEntries().Get(hash)
		_, okB := logB.GetEntries().Get(hash)

		if okA && !okB && eA.GetLogID() == logB.GetID() {
			res.Set(hash, eA)
			traversed[hash] = true
			for _, h := range eA.GetNext() {
				hash := h.String()
				_, okB := logB.GetEntries().Get(hash)
				_, okT := traversed[hash]
				if !okT && !okB {
					stack = append(stack, hash)
					traversed[hash] = true
				}
			}
		}
	}

	return res
}

// ToString Returns the log values as a nicely formatted string
//
// payloadMapper is a function to customize text representation,
// use nil to use the default mapper which convert the payload as a string
func (l *IPFSLog) ToString(payloadMapper func(iface.IPFSLogEntry) string) string {
	values := l.Values().Slice()
	sorting.Reverse(values)

	var lines []string

	for _, e := range values {
		parents := entry.FindChildren(e, l.Values().Slice())
		length := len(parents)
		padding := strings.Repeat("  ", maxInt(length-1, 0))
		if length > 0 {
			padding = padding + "└─"
		}

		payload := ""
		if payloadMapper != nil {
			payload = payloadMapper(e)
		} else {
			payload = string(e.GetPayload())
		}

		lines = append(lines, padding+payload)
	}

	return strings.Join(lines, "\n")
}

// ToSnapshot exports a Snapshot-able version of the log
func (l *IPFSLog) ToSnapshot() *Snapshot {
	l.lock.RLock()
	heads := l.heads.Slice()
	l.lock.RUnlock()

	return &Snapshot{
		ID:     l.ID,
		Heads:  entrySliceToCids(heads),
		Values: l.Values().Slice(),
	}
}

func entrySliceToCids(slice []iface.IPFSLogEntry) []cid.Cid {
	var cids []cid.Cid

	for _, e := range slice {
		cids = append(cids, e.GetHash())
	}

	return cids
}

//func (l *IPFSLog) toBuffer() ([]byte, error) {
//	return json.Marshal(l.ToJSON())
//}

// ToMultihash Returns the multihash of the log
//
// Converting the log to a multihash will persist the contents of
// log.toJSON to IPFS, thus causing side effects
//
// The only supported format is dag-cbor and a CIDv1 is returned
func (l *IPFSLog) ToMultihash(ctx context.Context) (cid.Cid, error) {
	return toMultihash(ctx, l.Storage, l)
}

// NewFromMultihash Creates a IPFSLog from a hash
//
// Creating a log from a hash will retrieve entries from IPFS, thus causing side effects
func NewFromMultihash(ctx context.Context, services io.IpfsServices, identity *identityprovider.Identity, hash cid.Cid, logOptions *LogOptions, fetchOptions *FetchOptions) (*IPFSLog, error) {
	if services == nil {
		return nil, errmsg.IPFSNotDefined
	}

	if identity == nil {
		return nil, errmsg.IdentityNotDefined
	}

	if logOptions == nil {
		return nil, errmsg.LogOptionsNotDefined
	}

	if fetchOptions == nil {
		return nil, errmsg.FetchOptionsNotDefined
	}

	data, err := fromMultihash(ctx, services, hash, &FetchOptions{
		Length:       fetchOptions.Length,
		Exclude:      fetchOptions.Exclude,
		ProgressChan: fetchOptions.ProgressChan,
	})

	if err != nil {
		return nil, errors.Wrap(err, "newfrommultihash failed")
	}

	var heads []iface.IPFSLogEntry
	for _, e := range data.Values {
		for _, h := range data.Heads {
			if e.GetHash().String() == h.String() {
				heads = append(heads, e)
				break
			}
		}
	}

	return NewLog(services, identity, &LogOptions{
		ID:               data.ID,
		AccessController: logOptions.AccessController,
		Entries:          entry.NewOrderedMapFromEntries(data.Values),
		Heads:            heads,
		Clock:            entry.NewLamportClock(data.Clock.GetID(), data.Clock.GetTime()),
		SortFn:           logOptions.SortFn,
	})
}

// NewFromEntryHash Creates a IPFSLog from a hash of an Entry
//
// Creating a log from a hash will retrieve entries from IPFS, thus causing side effects
func NewFromEntryHash(ctx context.Context, services io.IpfsServices, identity *identityprovider.Identity, hash cid.Cid, logOptions *LogOptions, fetchOptions *FetchOptions) (*IPFSLog, error) {
	if logOptions == nil {
		return nil, errmsg.LogOptionsNotDefined
	}

	if fetchOptions == nil {
		return nil, errmsg.FetchOptionsNotDefined
	}

	// TODO: need to verify the entries with 'key'
	entries, err := fromEntryHash(ctx, services, []cid.Cid{hash}, &FetchOptions{
		Length:       fetchOptions.Length,
		Exclude:      fetchOptions.Exclude,
		ProgressChan: fetchOptions.ProgressChan,
	})
	if err != nil {
		return nil, errors.Wrap(err, "newfromentryhash failed")
	}

	return NewLog(services, identity, &LogOptions{
		ID:               logOptions.ID,
		AccessController: logOptions.AccessController,
		Entries:          entry.NewOrderedMapFromEntries(entries),
		SortFn:           logOptions.SortFn,
	})
}

// NewFromJSON Creates a IPFSLog from a JSON Snapshot
//
// Creating a log from a JSON Snapshot will retrieve entries from IPFS, thus causing side effects
func NewFromJSON(ctx context.Context, services io.IpfsServices, identity *identityprovider.Identity, jsonLog *JSONLog, logOptions *LogOptions, fetchOptions *entry.FetchOptions) (*IPFSLog, error) {
	if logOptions == nil {
		return nil, errmsg.LogOptionsNotDefined
	}

	if fetchOptions == nil {
		return nil, errmsg.FetchOptionsNotDefined
	}

	// TODO: need to verify the entries with 'key'

	snapshot, err := fromJSON(ctx, services, jsonLog, &entry.FetchOptions{
		Length:       fetchOptions.Length,
		Timeout:      fetchOptions.Timeout,
		ProgressChan: fetchOptions.ProgressChan,
	})
	if err != nil {
		return nil, errors.Wrap(err, "newfromjson failed")
	}

	return NewLog(services, identity, &LogOptions{
		ID:               snapshot.ID,
		AccessController: logOptions.AccessController,
		Entries:          entry.NewOrderedMapFromEntries(snapshot.Values),
		SortFn:           logOptions.SortFn,
	})
}

// NewFromEntry Creates a IPFSLog from an Entry
//
// Creating a log from an entry will retrieve entries from IPFS, thus causing side effects
func NewFromEntry(ctx context.Context, services io.IpfsServices, identity *identityprovider.Identity, sourceEntries []iface.IPFSLogEntry, logOptions *LogOptions, fetchOptions *entry.FetchOptions) (*IPFSLog, error) {
	if logOptions == nil {
		return nil, errmsg.LogOptionsNotDefined
	}

	if fetchOptions == nil {
		return nil, errmsg.FetchOptionsNotDefined
	}

	// TODO: need to verify the entries with 'key'
	snapshot, err := fromEntry(ctx, services, sourceEntries, &entry.FetchOptions{
		Length:       fetchOptions.Length,
		Exclude:      fetchOptions.Exclude,
		ProgressChan: fetchOptions.ProgressChan,
	})
	if err != nil {
		return nil, errors.Wrap(err, "newfromentry failed")
	}

	return NewLog(services, identity, &LogOptions{
		ID:               snapshot.ID,
		AccessController: logOptions.AccessController,
		Entries:          entry.NewOrderedMapFromEntries(snapshot.Values),
		SortFn:           logOptions.SortFn,
	})
}

// Values Returns an Array of entries in the log
//
// The values are in linearized order according to their Lamport clocks
func (l *IPFSLog) Values() iface.IPFSLogOrderedEntries {
	l.lock.RLock()
	heads := l.heads
	l.lock.RUnlock()

	if heads == nil {
		return entry.NewOrderedMap()
	}
	stack, _ := l.traverse(heads, -1, "")
	sorting.Reverse(stack)

	return entry.NewOrderedMapFromEntries(stack)
}

// ToJSON Returns a log in a JSON serializable structure
func (l *IPFSLog) ToJSON() *JSONLog {
	l.lock.RLock()
	heads := l.heads
	l.lock.RUnlock()

	stack := heads.Slice()
	sorting.Sort(l.SortFn, stack)
	sorting.Reverse(stack)

	var hashes []cid.Cid
	for _, e := range stack {
		hashes = append(hashes, e.GetHash())
	}

	return &JSONLog{
		ID:    l.ID,
		Heads: hashes,
	}
}

func (l *IPFSLog) GetID() string {
	return l.ID
}

func (l *IPFSLog) GetEntries() iface.IPFSLogOrderedEntries {
	return l.Entries
}

// Heads Returns the heads of the log
//
// Heads are the entries that are not referenced by other entries in the log
func (l *IPFSLog) Heads() iface.IPFSLogOrderedEntries {
	l.lock.RLock()
	heads := l.heads.Slice()
	l.lock.RUnlock()

	sorting.Sort(l.SortFn, heads)
	sorting.Reverse(heads)

	return entry.NewOrderedMapFromEntries(heads)
}

var atlasJSONLog = atlas.BuildEntry(JSONLog{}).
	StructMap().
	AddField("ID", atlas.StructMapEntry{SerialName: "id"}).
	AddField("Heads", atlas.StructMapEntry{SerialName: "heads"}).
	Complete()

func init() {
	cbornode.RegisterCborType(atlasJSONLog)
}

var _ iface.IPFSLog = (*IPFSLog)(nil)
