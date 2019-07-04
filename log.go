// Package ipfslog implements an append-only log CRDT on IPFS
package ipfslog // import "berty.tech/go-ipfs-log"

import (
	"strconv"
	"strings"
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

type JSONLog struct {
	ID    string
	Heads []cid.Cid
}

type Log struct {
	Storage          io.IpfsServices
	ID               string
	AccessController accesscontroller.Interface
	SortFn           func(a *entry.Entry, b *entry.Entry) (int, error)
	Identity         *identityprovider.Identity
	Entries          *entry.OrderedMap
	heads            *entry.OrderedMap
	Next             *entry.OrderedMap
	Clock            *entry.LamportClock
}

type LogOptions struct {
	ID               string
	AccessController accesscontroller.Interface
	Entries          *entry.OrderedMap
	Heads            []*entry.Entry
	Clock            *entry.LamportClock
	SortFn           func(a *entry.Entry, b *entry.Entry) (int, error)
}

type Snapshot struct {
	ID     string
	Heads  []cid.Cid
	Values []*entry.Entry
	Clock  *entry.LamportClock
}

// maxInt Returns the larger of x or y
func maxInt(x, y int) int {
	if x < y {
		return y
	}
	return x
}

func maxClockTimeForEntries(entries []*entry.Entry, defValue int) int {
	max := defValue
	for _, e := range entries {
		max = maxInt(e.Clock.Time, max)
	}

	return max
}

// NewLog Creates creates a new Log for a given identity
//
// Each Log gets a unique ID, which can be passed in the options as ID.
//
// Returns a log instance.
//
// ipfs is an instance of IPFS.
//
// identity is an instance of Identity and will be used to sign entries
// Usually this should be a user id or similar.
//
// options.AccessController is an instance of accesscontroller.Interface,
// which by default allows anyone to append to the Log.
func NewLog(services io.IpfsServices, identity *identityprovider.Identity, options *LogOptions) (*Log, error) {
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
		maxTime = options.Clock.Time
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
		for _, n := range entry.Next {
			next.Set(n.String(), entry)
		}
	}

	return &Log{
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
func (l *Log) addToStack(e *entry.Entry, stack []*entry.Entry, traversed *orderedmap.OrderedMap) ([]*entry.Entry, *orderedmap.OrderedMap) {
	// If we've already processed the entry, don't add it to the stack
	if _, ok := traversed.Get(e.Hash.String()); ok {
		return stack, traversed
	}

	// Add the entry in front of the stack and sort
	stack = append([]*entry.Entry{e}, stack...)
	sorting.Sort(l.SortFn, stack)
	sorting.Reverse(stack)

	// Add to the cache of processed entries
	traversed.Set(e.Hash.String(), true)

	return stack, traversed
}

func (l *Log) traverse(rootEntries *entry.OrderedMap, amount int, endHash string) ([]*entry.Entry, error) {
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
	result := []*entry.Entry{}
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
		for _, next := range e.Next {
			nextEntry, ok := l.Entries.Get(next.String())
			if !ok {
				continue
			}

			stack, traversed = l.addToStack(nextEntry, stack, traversed)
		}

		// If it is the specified end hash, break out of the while loop
		if e.Hash.String() == endHash {
			break
		}
	}

	return result, nil
}

// Append Appends an entry to the log Returns the latest Entry
//
// payload is the data that will be in the Entry
func (l *Log) Append(payload []byte, pointerCount int) (*entry.Entry, error) {
	// INFO: JS default value for pointerCount is 1
	// Update the clock (find the latest clock)
	newTime := maxClockTimeForEntries(l.heads.Slice(), 0)
	newTime = maxInt(l.Clock.Time, newTime) + 1

	l.Clock = entry.NewLamportClock(l.Clock.ID, newTime)

	// Get the required amount of hashes to next entries (as per current state of the log)
	references, err := l.traverse(l.heads, maxInt(pointerCount, l.heads.Len()), "")
	if err != nil {
		return nil, errors.Wrap(err, "append failed")
	}

	next := []cid.Cid{}

	keys := l.heads.Keys()
	for _, k := range keys {
		e, _ := l.heads.Get(k)
		next = append(next, e.Hash)
	}
	for _, e := range references {
		next = append(next, e.Hash)
	}

	// TODO: ensure port of ```Object.keys(Object.assign({}, this._headsIndex, references))``` is correctly implemented

	// @TODO: Split Entry.create into creating object, checking permission, signing and then posting to IPFS
	// Create the entry and add it to the internal cache
	e, err := entry.CreateEntry(l.Storage, l.Identity, &entry.Entry{
		LogID:   l.ID,
		Payload: payload,
		Next:    next,
	}, l.Clock)
	if err != nil {
		return nil, errors.Wrap(err, "append failed")
	}

	if err := l.AccessController.CanAppend(e, l.Identity.Provider); err != nil {
		return nil, errors.Wrap(err, "append failed")
	}

	l.Entries.Set(e.Hash.String(), e)

	for _, k := range keys {
		nextEntry, _ := l.heads.Get(k)
		l.Next.Set(nextEntry.Hash.String(), e)
	}

	l.heads = entry.NewOrderedMap()
	l.heads.Set(e.Hash.String(), e)

	return e, nil
}

type IteratorOptions struct {
	GT     *cid.Cid
	GTE    *cid.Cid
	LT     []cid.Cid
	LTE    []cid.Cid
	Amount *int
}

/* Iterator Provides entries values on a channel */
func (l *Log) Iterator(options *IteratorOptions, output chan<- *entry.Entry) error {
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

	start := l.heads.Slice()
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
			for _, n := range e.Next {
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
func (l *Log) Join(otherLog *Log, size int) (*Log, error) {
	// INFO: JS default size is -1
	if otherLog == nil {
		return nil, errmsg.LogJoinNotDefined
	}

	if l.ID != otherLog.ID {
		return l, nil
	}

	newItems := difference(otherLog, l)

	for _, k := range newItems.Keys() {
		e := newItems.UnsafeGet(k)
		if err := l.AccessController.CanAppend(e, l.Identity.Provider); err != nil {
			return nil, errors.Wrap(err, "join failed")
		}

		if err := e.Verify(l.Identity.Provider); err != nil {
			return nil, errors.Wrap(err, "unable to check signature")
		}
	}

	for _, k := range newItems.Keys() {
		e := newItems.UnsafeGet(k)
		for _, next := range e.Next {
			l.Next.Set(next.String(), e)
		}

		l.Entries.Set(e.Hash.String(), e)
	}

	nextsFromNewItems := orderedmap.New()
	for _, k := range newItems.Keys() {
		e := newItems.UnsafeGet(k)
		for _, n := range e.Next {
			nextsFromNewItems.Set(n.String(), true)
		}
	}

	mergedHeads := entry.FindHeads(l.heads.Merge(otherLog.heads))
	for idx, e := range mergedHeads {
		// notReferencedByNewItems
		if _, ok := nextsFromNewItems.Get(e.Hash.String()); ok {
			mergedHeads[idx] = nil
		}

		// notInCurrentNexts
		if _, ok := l.Next.Get(e.Hash.String()); ok {
			mergedHeads[idx] = nil
		}
	}

	l.heads = entry.NewOrderedMapFromEntries(mergedHeads)

	if size > -1 {
		tmp := l.Values().Slice()
		tmp = tmp[len(tmp)-size:]
		l.Entries = entry.NewOrderedMapFromEntries(tmp)
		l.heads = entry.NewOrderedMapFromEntries(entry.FindHeads(entry.NewOrderedMapFromEntries(tmp)))
	}

	// Find the latest clock from the heads
	maxClock := maxClockTimeForEntries(l.heads.Slice(), 0)
	l.Clock = entry.NewLamportClock(l.Clock.ID, maxInt(l.Clock.Time, maxClock))

	return l, nil
}

func difference(logA, logB *Log) *entry.OrderedMap {
	if logA == nil || logA.Entries == nil || logA.Entries.Len() == 0 || logB == nil {
		return entry.NewOrderedMap()
	}

	if logB.Entries == nil {
		logB.Entries = entry.NewOrderedMap()
	}

	stack := logA.heads.Keys()
	traversed := map[string]bool{}
	res := entry.NewOrderedMap()

	for {
		if len(stack) == 0 {
			break
		}
		hash := stack[0]
		stack = stack[1:]

		eA, okA := logA.Entries.Get(hash)
		_, okB := logB.Entries.Get(hash)

		if okA && !okB && eA.LogID == logB.ID {
			res.Set(hash, eA)
			traversed[hash] = true
			for _, h := range eA.Next {
				hash := h.String()
				_, okB := logB.Entries.Get(hash)
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
func (l *Log) ToString(payloadMapper func(*entry.Entry) string) string {
	values := l.Values().Slice()
	sorting.Reverse(values)

	lines := []string{}

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
			payload = string(e.Payload)
		}

		lines = append(lines, padding+payload)
	}

	return strings.Join(lines, "\n")
}

// ToSnapshot exports a Snapshot-able version of the log
func (l *Log) ToSnapshot() *Snapshot {
	return &Snapshot{
		ID:     l.ID,
		Heads:  entrySliceToCids(l.heads.Slice()),
		Values: l.Values().Slice(),
	}
}

func entrySliceToCids(slice []*entry.Entry) []cid.Cid {
	var cids []cid.Cid

	for _, e := range slice {
		cids = append(cids, e.Hash)
	}

	return cids
}

//func (l *Log) toBuffer() ([]byte, error) {
//	return json.Marshal(l.ToJSON())
//}

// ToMultihash Returns the multihash of the log
//
// Converting the log to a multihash will persist the contents of
// log.toJSON to IPFS, thus causing side effects
//
// The only supported format is dag-cbor and a CIDv1 is returned
func (l *Log) ToMultihash() (cid.Cid, error) {
	return toMultihash(l.Storage, l)
}

// NewFromMultihash Creates a Log from a hash
//
// Creating a log from a hash will retrieve entries from IPFS, thus causing side effects
func NewFromMultihash(services io.IpfsServices, identity *identityprovider.Identity, hash cid.Cid, logOptions *LogOptions, fetchOptions *FetchOptions) (*Log, error) {
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

	data, err := fromMultihash(services, hash, &FetchOptions{
		Length:       fetchOptions.Length,
		Exclude:      fetchOptions.Exclude,
		ProgressChan: fetchOptions.ProgressChan,
	})

	if err != nil {
		return nil, errors.Wrap(err, "newfrommultihash failed")
	}

	heads := []*entry.Entry{}
	for _, e := range data.Values {
		for _, h := range data.Heads {
			if e.Hash.String() == h.String() {
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
		Clock:            entry.NewLamportClock(data.Clock.ID, data.Clock.Time),
		SortFn:           logOptions.SortFn,
	})
}

// NewFromEntryHash Creates a Log from a hash of an Entry
//
// Creating a log from a hash will retrieve entries from IPFS, thus causing side effects
func NewFromEntryHash(services io.IpfsServices, identity *identityprovider.Identity, hash cid.Cid, logOptions *LogOptions, fetchOptions *FetchOptions) (*Log, error) {
	if logOptions == nil {
		return nil, errmsg.LogOptionsNotDefined
	}

	if fetchOptions == nil {
		return nil, errmsg.FetchOptionsNotDefined
	}

	// TODO: need to verify the entries with 'key'
	entries, err := fromEntryHash(services, []cid.Cid{hash}, &FetchOptions{
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

// NewFromJSON Creates a Log from a JSON Snapshot
//
// Creating a log from a JSON Snapshot will retrieve entries from IPFS, thus causing side effects
func NewFromJSON(services io.IpfsServices, identity *identityprovider.Identity, jsonLog *JSONLog, logOptions *LogOptions, fetchOptions *entry.FetchOptions) (*Log, error) {
	if logOptions == nil {
		return nil, errmsg.LogOptionsNotDefined
	}

	if fetchOptions == nil {
		return nil, errmsg.FetchOptionsNotDefined
	}

	// TODO: need to verify the entries with 'key'

	snapshot, err := fromJSON(services, jsonLog, &entry.FetchOptions{
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

// NewFromEntry Creates a Log from an Entry
//
// Creating a log from an entry will retrieve entries from IPFS, thus causing side effects
func NewFromEntry(services io.IpfsServices, identity *identityprovider.Identity, sourceEntries []*entry.Entry, logOptions *LogOptions, fetchOptions *entry.FetchOptions) (*Log, error) {
	if logOptions == nil {
		return nil, errmsg.LogOptionsNotDefined
	}

	if fetchOptions == nil {
		return nil, errmsg.FetchOptionsNotDefined
	}

	// TODO: need to verify the entries with 'key'
	snapshot, err := fromEntry(services, sourceEntries, &entry.FetchOptions{
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
func (l *Log) Values() *entry.OrderedMap {
	if l.heads == nil {
		return entry.NewOrderedMap()
	}
	stack, _ := l.traverse(l.heads, -1, "")
	sorting.Reverse(stack)

	return entry.NewOrderedMapFromEntries(stack)
}

// ToJSON Returns a log in a JSON serializable structure
func (l *Log) ToJSON() *JSONLog {
	stack := l.heads.Slice()
	sorting.Sort(l.SortFn, stack)
	sorting.Reverse(stack)

	hashes := []cid.Cid{}
	for _, e := range stack {
		hashes = append(hashes, e.Hash)
	}

	return &JSONLog{
		ID:    l.ID,
		Heads: hashes,
	}
}

// Heads Returns the heads of the log
//
// Heads are the entries that are not referenced by other entries in the log
func (l *Log) Heads() *entry.OrderedMap {
	heads := l.heads.Slice()
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
