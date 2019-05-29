package log

import (
	"bytes"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/iancoleman/orderedmap"

	cbornode "github.com/ipfs/go-ipld-cbor"
	"github.com/polydawn/refmt/obj/atlas"

	"github.com/berty/go-ipfs-log/accesscontroler"
	"github.com/berty/go-ipfs-log/entry"
	"github.com/berty/go-ipfs-log/errmsg"
	"github.com/berty/go-ipfs-log/identityprovider"
	"github.com/berty/go-ipfs-log/io"
	"github.com/berty/go-ipfs-log/utils/lamportclock"
	"github.com/ipfs/go-cid"
	"github.com/pkg/errors"
)

type JSONLog struct {
	ID    string
	Heads []cid.Cid
}

type Log struct {
	Storage          *io.IpfsServices
	ID               string
	AccessController accesscontroler.Interface
	SortFn           func(a *entry.Entry, b *entry.Entry) (int, error)
	Identity         *identityprovider.Identity
	Entries          *entry.OrderedMap
	Heads            *entry.OrderedMap
	Next             *entry.OrderedMap
	Clock            *lamportclock.LamportClock
}

type NewLogOptions struct {
	ID               string
	AccessController accesscontroler.Interface
	Entries          *entry.OrderedMap
	Heads            []*entry.Entry
	Clock            *lamportclock.LamportClock
	SortFn           func(a *entry.Entry, b *entry.Entry) (int, error)
}

type Snapshot struct {
	ID     string
	Heads  []cid.Cid
	Values []*entry.Entry
	Clock  *lamportclock.LamportClock
}

// max returns the larger of x or y.
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

func NewLog(services *io.IpfsServices, identity *identityprovider.Identity, options *NewLogOptions) (*Log, error) {
	if services == nil {
		return nil, errmsg.IPFSNotDefined
	}

	if identity == nil {
		return nil, errmsg.IdentityNotDefined
	}

	if options == nil {
		options = &NewLogOptions{}
	}

	if options.ID == "" {
		options.ID = strconv.FormatInt(time.Now().Unix()/1000, 10)
	}

	if options.SortFn == nil {
		options.SortFn = LastWriteWins
	}

	maxTime := 0
	if options.Clock != nil {
		maxTime = options.Clock.Time
	}
	maxTime = maxClockTimeForEntries(options.Heads, maxTime)

	if options.AccessController == nil {
		options.AccessController = &accesscontroler.Default{}
	}

	if options.Entries == nil {
		options.Entries = entry.NewOrderedMap()
	}

	if len(options.Heads) == 0 && len(options.Entries.Keys()) > 0 {
		options.Heads = FindHeads(options.Entries)
	}

	return &Log{
		Storage:          services,
		ID:               options.ID,
		Identity:         identity,
		AccessController: options.AccessController,
		SortFn:           NoZeroes(options.SortFn),
		Entries:          options.Entries.Copy(),
		Heads:            entry.NewOrderedMapFromEntries(options.Heads),
		Next:             entry.NewOrderedMap(),
		Clock:            lamportclock.New(identity.PublicKey, maxTime),
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
	sort.SliceStable(stack, Sortable(l.SortFn, stack))
	reverse(stack)

	// Add to the cache of processed entries
	traversed.Set(e.Hash.String(), true)

	return stack, traversed
}

func (l *Log) Traverse(rootEntries *entry.OrderedMap, amount int, endHash string) ([]*entry.Entry, error) {
	if rootEntries == nil {
		return nil, errmsg.EntriesNotDefined
	}

	// Sort the given given root entries and use as the starting stack
	stack := rootEntries.Slice()

	sort.SliceStable(stack, Sortable(l.SortFn, stack))
	reverse(stack)

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

func (l *Log) Append(payload []byte, pointerCount int) (*entry.Entry, error) {
	// Update the clock (find the latest clock)
	newTime := maxClockTimeForEntries(l.Heads.Slice(), 0)
	newTime = maxInt(l.Clock.Time, newTime) + 1

	l.Clock = lamportclock.New(l.Clock.ID, newTime)

	// Get the required amount of hashes to next entries (as per current state of the log)
	references, err := l.Traverse(l.Heads, maxInt(pointerCount, l.Heads.Len()), "")
	if err != nil {
		return nil, errors.Wrap(err, "append failed")
	}

	next := []cid.Cid{}

	keys := l.Heads.Keys()
	for _, k := range keys {
		e, _ := l.Heads.Get(k)
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

	if err := l.AccessController.CanAppend(e, l.Identity); err != nil {
		return nil, errors.Wrap(err, "append failed")
	}

	l.Entries.Set(e.Hash.String(), e)

	for _, k := range keys {
		nextEntry, _ := l.Heads.Get(k)
		l.Next.Set(nextEntry.Hash.String(), e)
	}

	l.Heads = entry.NewOrderedMap()
	l.Heads.Set(e.Hash.String(), e)

	return e, nil
}

type IteratorOptions struct {
	GT     *entry.Entry
	GTE    *entry.Entry
	LT     *entry.Entry
	LTE    *entry.Entry
	Amount *int
}

func (l *Log) iterator(options IteratorOptions, output chan<- *entry.Entry) error {
	amount := -1
	if options.Amount != nil {
		if *options.Amount == 0 {
			return nil
		} else {
			amount = *options.Amount
		}
	}

	start := l.Heads.Slice()
	if options.LTE != nil {
		start = []*entry.Entry{options.LTE}
	} else if options.LT != nil {
		start = []*entry.Entry{options.LT}
	}

	endHash := ""
	if options.GTE != nil {
		endHash = options.GTE.Hash.String()
	} else if options.GT != nil {
		endHash = options.GT.Hash.String()
	}

	count := -1
	if endHash == "" && options.Amount != nil {
		count = amount
	}

	entries, err := l.Traverse(entry.NewOrderedMapFromEntries(start), count, endHash)
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

	return nil
}

func (l *Log) Join(otherLog *Log, size int) (*Log, error) {
	if otherLog == nil {
		return nil, errmsg.LogJoinNotDefined
	}

	if l.ID != otherLog.ID {
		return l, nil
	}

	newItems := Difference(otherLog, l)

	for _, k := range newItems.Keys() {
		e := newItems.UnsafeGet(k)
		if err := l.AccessController.CanAppend(e, l.Identity); err != nil {
			return nil, errors.Wrap(err, "join failed")
		}

		if err := entry.Verify(l.Identity.Provider, e); err != nil {
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

	mergedHeads := FindHeads(l.Heads.Merge(otherLog.Heads))
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

	l.Heads = entry.NewOrderedMapFromEntries(mergedHeads)

	if size > -1 {
		tmp := l.Values().Slice()
		tmp = tmp[len(tmp)-size:]
		l.Entries = entry.NewOrderedMapFromEntries(tmp)
		l.Heads = entry.NewOrderedMapFromEntries(FindHeads(entry.NewOrderedMapFromEntries(tmp)))
	}

	// Find the latest clock from the heads
	maxClock := maxClockTimeForEntries(l.Heads.Slice(), 0)
	l.Clock = lamportclock.New(l.Clock.ID, maxInt(l.Clock.Time, maxClock))

	return l, nil
}

// TODO DELETE THIS
func printEntries(entries *entry.OrderedMap) {
	for _, k := range entries.Keys() {
		entry := entries.UnsafeGet(k)
		printEntry(entry)
	}
}

// TODO DELETE THIS
func printEntry(entry *entry.Entry) {
	fmt.Println("Hash:", entry.Hash.String())
	fmt.Println("LogID:", entry.LogID)
	fmt.Println("Payload:", string(entry.Payload))
	fmt.Println("Next size:", len(entry.Next))
	fmt.Println("V:", entry.V)
	fmt.Println("Key:", hex.EncodeToString(entry.Key))
	fmt.Println("Sig:", hex.EncodeToString(entry.Sig))
	fmt.Println("Clock time:", entry.Clock.Time)
	fmt.Println("")
}

func Difference(logA, logB *Log) *entry.OrderedMap {
	if logA == nil || logA.Entries == nil || len(logA.Entries.Keys()) == 0 {
		return logB.Entries
	} else if logB == nil || logB.Entries == nil || len(logB.Entries.Keys()) == 0 {
		return logA.Entries
	}

	stack := logA.Heads.Keys()
	traversed := map[string]bool{}
	res := entry.NewOrderedMap()

	for {
		hash := stack[0]
		eA, okA := logA.Entries.Get(hash)
		_, okB := logB.Entries.Get(hash)

		if okA && !okB && eA.LogID == logA.ID {
			res.Set(hash, eA)
			traversed[hash] = true
			if eA.Next != nil {
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

		if len(stack) == 1 {
			break
		}
		stack = stack[1:]
	}

	return res
}

func (l *Log) ToString(payloadMapper func(*entry.Entry) string) string {
	values := l.Values().Slice()
	reverse(values)

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

func (l *Log) ToSnapshot() *Snapshot {
	return &Snapshot{
		ID:     l.ID,
		Heads:  entrySliceToCids(l.Heads.Slice()),
		Values: l.Values().Slice(),
	}
}

func entrySliceToCids(slice []*entry.Entry) []cid.Cid {
	cids := []cid.Cid{}

	for _, e := range slice {
		cids = append(cids, e.Hash)
	}

	return cids
}

func (l *Log) ToBuffer() ([]byte, error) {
	return json.Marshal(l.ToJSON())
}

func (l *Log) ToMultihash() (cid.Cid, error) {
	return ToMultihash(l.Storage, l)
}

func NewFromMultihash(services *io.IpfsServices, identity *identityprovider.Identity, hash cid.Cid, logOptions *NewLogOptions, fetchOptions *FetchOptions) (*Log, error) {
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

	data, err := FromMultihash(services, hash, &FetchOptions{
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

	return NewLog(services, identity, &NewLogOptions{
		ID:               data.ID,
		AccessController: logOptions.AccessController,
		Entries:          entry.NewOrderedMapFromEntries(data.Values),
		Heads:            heads,
		Clock:            lamportclock.New(data.Clock.ID, data.Clock.Time),
		SortFn:           logOptions.SortFn,
	})
}

func NewFromEntryHash(services *io.IpfsServices, identity *identityprovider.Identity, hash cid.Cid, logOptions *NewLogOptions, fetchOptions *FetchOptions) (*Log, error) {
	if logOptions == nil {
		return nil, errmsg.LogOptionsNotDefined
	}

	if fetchOptions == nil {
		return nil, errmsg.FetchOptionsNotDefined
	}

	// TODO: need to verify the entries with 'key'
	entries, err := FromEntryHash(services, []cid.Cid{hash}, &FetchOptions{
		Length:       fetchOptions.Length,
		Exclude:      fetchOptions.Exclude,
		ProgressChan: fetchOptions.ProgressChan,
	})
	if err != nil {
		return nil, errors.Wrap(err, "newfromentryhash failed")
	}

	return NewLog(services, identity, &NewLogOptions{
		ID:               logOptions.ID,
		AccessController: logOptions.AccessController,
		Entries:          entry.NewOrderedMapFromEntries(entries),
		SortFn:           logOptions.SortFn,
	})
}

func NewFromJSON(services *io.IpfsServices, identity *identityprovider.Identity, jsonData []byte, logOptions *NewLogOptions, fetchOptions *entry.FetchOptions) (*Log, error) {
	if logOptions == nil {
		return nil, errmsg.LogOptionsNotDefined
	}

	if fetchOptions == nil {
		return nil, errmsg.FetchOptionsNotDefined
	}

	// TODO: need to verify the entries with 'key'
	jsonLog := JSONLog{}

	snapshot, err := FromJSON(services, jsonLog, &entry.FetchOptions{
		Length:       fetchOptions.Length,
		Timeout:      fetchOptions.Timeout,
		ProgressChan: fetchOptions.ProgressChan,
	})
	if err != nil {
		return nil, errors.Wrap(err, "newfromjson failed")
	}

	return NewLog(services, identity, &NewLogOptions{
		ID:               logOptions.ID,
		AccessController: logOptions.AccessController,
		Entries:          entry.NewOrderedMapFromEntries(snapshot.Values),
		SortFn:           logOptions.SortFn,
	})
}

func NewFromEntry(services *io.IpfsServices, identity *identityprovider.Identity, sourceEntries []*entry.Entry, logOptions *NewLogOptions, fetchOptions *entry.FetchOptions) (*Log, error) {
	if logOptions == nil {
		return nil, errmsg.LogOptionsNotDefined
	}

	if fetchOptions == nil {
		return nil, errmsg.FetchOptionsNotDefined
	}

	// TODO: need to verify the entries with 'key'
	snapshot, err := FromEntry(services, sourceEntries, &entry.FetchOptions{
		Length:       fetchOptions.Length,
		Exclude:      fetchOptions.Exclude,
		ProgressChan: fetchOptions.ProgressChan,
	})
	if err != nil {
		return nil, errors.Wrap(err, "newfromentry failed")
	}

	return NewLog(services, identity, &NewLogOptions{
		ID:               logOptions.ID,
		AccessController: logOptions.AccessController,
		Entries:          entry.NewOrderedMapFromEntries(snapshot.Values),
		SortFn:           logOptions.SortFn,
	})
}

func FindTails(entries []*entry.Entry) []*entry.Entry {
	// Reverse index { next -> entry }
	reverseIndex := map[string][]*entry.Entry{}
	// Null index containing entries that have no parents (nexts)
	nullIndex := []*entry.Entry{}
	// Hashes for all entries for quick lookups
	hashes := map[string]bool{}
	// Hashes of all next entries
	nexts := []cid.Cid{}

	for _, e := range entries {
		if len(e.Next) == 0 {
			nullIndex = append(nullIndex, e)
		}

		for _, nextE := range e.Next {
			reverseIndex[nextE.String()] = append(reverseIndex[nextE.String()], e)
		}

		nexts = append(nexts, e.Next...)

		hashes[e.Hash.String()] = true
	}

	tails := []*entry.Entry{}

	for _, n := range nexts {
		if _, ok := hashes[n.String()]; !ok {
			continue
		}

		tails = append(tails, reverseIndex[n.String()]...)
	}

	tails = append(tails, nullIndex...)

	return entry.NewOrderedMapFromEntries(tails).Slice()
}

func FindTailHashes(entries []*entry.Entry) []string {
	res := []string{}
	hashes := map[string]bool{}
	for _, e := range entries {
		hashes[e.Hash.String()] = true
	}

	for _, e := range entries {
		nextLength := len(e.Next)

		for i := range e.Next {
			next := e.Next[nextLength-i]
			if _, ok := hashes[next.String()]; !ok {
				res = append([]string{e.Hash.String()}, res...)
			}
		}
	}

	return res
}

func FindHeads(entries *entry.OrderedMap) []*entry.Entry {
	if entries == nil {
		return nil
	}

	result := []*entry.Entry{}
	entriesWithParents := orderedmap.New()

	for _, h := range entries.Keys() {
		e, ok := entries.Get(h)
		if !ok || e == nil {
			continue
		}

		if _, ok := entriesWithParents.Get(h); !ok {
			entriesWithParents.Set(h, false)
		}

		for _, n := range e.Next {
			entriesWithParents.Set(n.String(), true)
		}
	}

	keys := entriesWithParents.Keys()
	for _, h := range keys {
		val, ok := entriesWithParents.Get(h)
		if !ok {
			continue
		}

		hasParent, ok := val.(bool)
		if !ok {
			continue
		}

		if !hasParent {
			result = append(result, entries.UnsafeGet(h))
		}
	}

	sort.SliceStable(result, func(a, b int) bool {
		eA, _ := entries.Get(result[a].Hash.String())
		eB, _ := entries.Get(result[b].Hash.String())

		return bytes.Compare(eA.Clock.ID, eB.Clock.ID) <= 0
	})

	return result
}

func (l *Log) Values() *entry.OrderedMap {
	if l.Heads == nil {
		return entry.NewOrderedMap()
	}
	stack, _ := l.Traverse(l.Heads, -1, "")
	sort.SliceStable(stack, Sortable(l.SortFn, stack))

	return entry.NewOrderedMapFromEntries(stack)
}

func (l *Log) ToJSON() *JSONLog {
	stack := l.Heads.Slice()
	sort.SliceStable(stack, Sortable(l.SortFn, stack))
	reverse(stack)

	hashes := []cid.Cid{}
	for _, e := range stack {
		hashes = append(hashes, e.Hash)
	}

	return &JSONLog{
		ID:    l.ID,
		Heads: hashes,
	}
}

var AtlasJSONLog = atlas.BuildEntry(JSONLog{}).
	StructMap().
	AddField("ID", atlas.StructMapEntry{SerialName: "id"}).
	AddField("Heads", atlas.StructMapEntry{SerialName: "heads"}).
	Complete()

func init() {
	cbornode.RegisterCborType(AtlasJSONLog)
}
