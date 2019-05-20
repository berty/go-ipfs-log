package log

import (
	"bytes"
	"encoding/json"
	"fmt"
	"sort"
	"strings"
	"time"

	"github.com/berty/go-ipfs-log/accesscontroler"
	"github.com/berty/go-ipfs-log/entry"
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
	Entries          map[string]*entry.Entry
	Heads            map[string]*entry.Entry
	Next             map[string]*entry.Entry
	Clock            *lamportclock.LamportClock
}

type NewLogOptions struct {
	ID               string
	AccessController accesscontroler.Interface
	Entries          []*entry.Entry
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

func entryMapToSlice(entries map[string]*entry.Entry) []*entry.Entry {
	ret := []*entry.Entry{}

	for _, e := range entries {
		ret = append(ret, e)
	}

	return ret
}

func mapUniqueEntries(entries []*entry.Entry) map[string]*entry.Entry {
	res := map[string]*entry.Entry{}
	for _, e := range entries {
		if e == nil {
			continue
		}

		res[e.Hash.String()] = e
	}

	return res
}

func NewLog(services *io.IpfsServices, identity *identityprovider.Identity, options *NewLogOptions) *Log {
	if options.ID == "" {
		options.ID = time.Now().String()
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

	return &Log{
		Storage:          services,
		ID:               options.ID,
		Identity:         identity,
		AccessController: options.AccessController,
		SortFn:           NoZeroes(options.SortFn),
		Entries:          mapUniqueEntries(options.Entries),
		Heads:            mapUniqueEntries(options.Heads),
		Clock:            lamportclock.New(identity.PublicKey, maxTime),
	}
}

// addToStack Add an entry to the stack and traversed nodes index
func (l *Log) addToStack(e *entry.Entry, stack []*entry.Entry, traversed map[string]bool) ([]*entry.Entry, map[string]bool) {
	// If we've already processed the entry, don't add it to the stack
	if _, ok := traversed[e.Hash.String()]; ok {
		return stack, traversed
	}

	// Add the entry in front of the stack and sort
	stack = append([]*entry.Entry{e}, stack...)
	sort.SliceStable(stack, Sortable(l.SortFn, stack))
	reverse(stack)

	// Add to the cache of processed entries
	traversed[e.Hash.String()] = true

	return stack, traversed
}

func (l *Log) Traverse(rootEntries map[string]*entry.Entry, amount int, endHash string) map[string]*entry.Entry {
	// Sort the given given root entries and use as the starting stack
	stack := entryMapToSlice(rootEntries)

	sort.SliceStable(stack, Sortable(l.SortFn, stack))
	reverse(stack)

	// Cache for checking if we've processed an entry already
	traversed := map[string]bool{}
	// End result
	result := map[string]*entry.Entry{}
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
		result[e.Hash.String()] = e

		// Add entry's next references to the stack
		for _, next := range e.Next {
			nextEntry, ok := l.Entries[next.String()]
			if !ok {
				continue
			}

			l.addToStack(nextEntry, stack, traversed)
		}

		// If it is the specified end hash, break out of the while loop
		if e.Hash.String() == endHash {
			break
		}
	}

	return result
}

func (l *Log) Append(payload []byte, pointerCount int) (*entry.Entry, error) {
	// Update the clock (find the latest clock)
	newTime := maxClockTimeForEntries(entryMapToSlice(l.Heads), 0)
	newTime = maxInt(l.Clock.Time, newTime) + 1

	l.Clock = lamportclock.New(l.Clock.ID, newTime)

	// Get the required amount of hashes to next entries (as per current state of the log)
	l.Traverse(l.Heads, maxInt(pointerCount, len(l.Heads)), "")
	next := []cid.Cid{}

	for _, e := range l.Heads {
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
		return nil, err
	}

	if err := l.AccessController.CanAppend(e, l.Identity); err != nil {
		return nil, errors.New("Could not append entry, key is not allowed to write to the log")
	}

	l.Entries[e.Hash.String()] = e
	for _, nextEntry := range l.Heads {
		l.Next[nextEntry.Hash.String()] = e
	}

	l.Heads = map[string]*entry.Entry{e.Hash.String(): e}

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

	start := entryMapToSlice(l.Heads)
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

	entries := entryMapToSlice(l.Traverse(mapUniqueEntries(start), count, endHash))

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
	if l.ID != otherLog.ID {
		return l, nil
	}

	newItems := Difference(otherLog, l)

	for _, e := range newItems {
		if err := l.AccessController.CanAppend(e, l.Identity); err != nil {
			return nil, errors.Wrap(err, "could not append entry, key is not allowed to write to the log")
		}

		if err := entry.Verify(l.Identity.Provider, e); err != nil {
			return nil, err
		}
	}

	for _, e := range newItems {
		for _, next := range e.Next {
			l.Next[next.String()] = e
		}

		l.Entries[e.Hash.String()] = e
	}

	nextsFromNewItems := map[string]bool{}
	for _, e := range newItems {
		for _, n := range e.Next {
			nextsFromNewItems[n.String()] = true
		}
	}

	mergedHeads := FindHeads(concatEntryMaps(l.Heads, otherLog.Heads))
	for idx, e := range mergedHeads {
		if _, ok := nextsFromNewItems[e.Hash.String()]; ok {
			mergedHeads[idx] = nil
		} else if _, ok := l.Next[e.Hash.String()]; !ok {
			mergedHeads[idx] = nil
		}
	}

	l.Heads = mapUniqueEntries(mergedHeads)

	if size > -1 {
		tmp := l.Values()
		tmp = tmp[len(tmp)-size:]
		l.Entries = mapUniqueEntries(tmp)
		l.Heads = mapUniqueEntries(FindHeads(mapUniqueEntries(tmp)))
	}

	// Find the latest clock from the heads
	maxClock := maxClockTimeForEntries(mergedHeads, 0)
	l.Clock = lamportclock.New(l.Clock.ID, maxInt(l.Clock.Time, maxClock))

	return l, nil
}

func Difference(logA, logB *Log) map[string]*entry.Entry {
	stack := []string{}
	traversed := map[string]bool{}
	res := map[string]*entry.Entry{}

	for k := range logA.Heads {
		stack = append(stack, k)
	}

	for len(stack) > 0 {
		hash := stack[0]
		stack = stack[1:]

		e, okA := logA.Entries[hash]
		_, okB := logB.Entries[hash]

		if okA && !okB && e.LogID == logB.ID {
			res[e.Hash.String()] = e
			traversed[e.Hash.String()] = true

			for _, next := range e.Next {
				if _, ok := traversed[next.String()]; !ok {
					stack = append(stack, next.String())
					traversed[next.String()] = true
				}
			}
		}

	}

	return res
}

func (l *Log) toString(payloadMapper func(*entry.Entry) string) string {
	values := l.Values()
	reverse(values)

	lines := []string{}

	for _, e := range values {
		parents := entry.FindChildren(e, l.Values())
		length := len(parents)
		padding := strings.Repeat("  ", maxInt(length-1, 0))
		if length > 0 {
			padding = padding + "└─"
		}

		payload := ""
		if payloadMapper != nil {
			payload = payloadMapper(e)
		} else {
			payload = fmt.Sprintf("%v", e.Payload)
		}

		return padding + payload
	}

	return strings.Join(lines, "\n")
}

func (l *Log) ToSnapshot() *Snapshot {
	return &Snapshot{
		ID:     l.ID,
		Heads:  entrySliceToCids(entryMapToSlice(l.Heads)),
		Values: l.Values(),
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
	data, err := FromMultihash(services, hash, &FetchOptions{
		Length:       fetchOptions.Length,
		Exclude:      fetchOptions.Exclude,
		ProgressChan: fetchOptions.ProgressChan,
	})

	if err != nil {
		return nil, err
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
		Entries:          data.Values,
		Heads:            heads,
		Clock:            lamportclock.New(data.Clock.ID, data.Clock.Time),
		SortFn:           logOptions.SortFn,
	}), nil
}

func NewFromEntryHash(services *io.IpfsServices, identity *identityprovider.Identity, hash cid.Cid, logOptions *NewLogOptions, fetchOptions *FetchOptions) *Log {
	// TODO: need to verify the entries with 'key'
	entries := FromEntryHash(services, []cid.Cid{hash}, &FetchOptions{
		Length:       fetchOptions.Length,
		Exclude:      fetchOptions.Exclude,
		ProgressChan: fetchOptions.ProgressChan,
	})

	return NewLog(services, identity, &NewLogOptions{
		ID:               logOptions.ID,
		AccessController: logOptions.AccessController,
		Entries:          entries,
		SortFn:           logOptions.SortFn,
	})
}

func NewFromJSON(services *io.IpfsServices, identity *identityprovider.Identity, jsonData []byte, logOptions *NewLogOptions, fetchOptions *entry.FetchOptions) *Log {
	// TODO: need to verify the entries with 'key'
	jsonLog := JSONLog{}

	snapshot := FromJSON(services, jsonLog, &entry.FetchOptions{
		Length:       fetchOptions.Length,
		Timeout:      fetchOptions.Timeout,
		ProgressChan: fetchOptions.ProgressChan,
	})

	return NewLog(services, identity, &NewLogOptions{
		ID:               logOptions.ID,
		AccessController: logOptions.AccessController,
		Entries:          snapshot.Values,
		SortFn:           logOptions.SortFn,
	})
}

func NewFromEntry(services *io.IpfsServices, identity *identityprovider.Identity, sourceEntries []*entry.Entry, logOptions *NewLogOptions, fetchOptions *entry.FetchOptions) *Log {
	// TODO: need to verify the entries with 'key'
	snapshot := FromEntry(services, sourceEntries, &entry.FetchOptions{
		Length:       fetchOptions.Length,
		Exclude:      fetchOptions.Exclude,
		ProgressChan: fetchOptions.ProgressChan,
	})

	return NewLog(services, identity, &NewLogOptions{
		ID:               logOptions.ID,
		AccessController: logOptions.AccessController,
		Entries:          snapshot.Values,
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

	return entryMapToSlice(mapUniqueEntries(tails))
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

func concatEntryMaps(sets ...map[string]*entry.Entry) map[string]*entry.Entry {
	result := map[string]*entry.Entry{}

	for _, set := range sets {
		for k, e := range set {
			result[k] = e
		}
	}

	return result
}

func FindHeads(entries map[string]*entry.Entry) []*entry.Entry {
	result := []*entry.Entry{}
	items := map[string]*entry.Entry{}

	for _, e := range entries {
		items[e.Hash.String()] = e

		for _, n := range e.Next {
			if nEntry, ok := items[n.String()]; !ok && nEntry != nil {
				items[n.String()] = nil
			}
		}
	}

	for _, e := range entries {
		if sub, ok := items[e.Hash.String()]; ok == true && sub != nil {
			continue
		}

		result = append(result, e)
	}

	sort.SliceStable(result, func(a, b int) bool {
		bytesA, _ := entries[result[a].Hash.String()].Clock.ID.Bytes()
		bytesB, _ := entries[result[b].Hash.String()].Clock.ID.Bytes()

		return bytes.Compare(bytesA, bytesB) > 0
	})

	return result
}

func (l *Log) Values() []*entry.Entry {
	entries := l.Traverse(l.Heads, -1, "")
	stack := entryMapToSlice(entries)
	reverse(stack)

	return stack
}

func (l *Log) ToJSON() *JSONLog {
	stack := entryMapToSlice(l.Heads)
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
