package ipfslog // import "berty.tech/go-ipfs-log"

import (
	"context"
	"time"

	core_iface "github.com/ipfs/interface-go-ipfs-core"

	"berty.tech/go-ipfs-log/iface"

	"berty.tech/go-ipfs-log/entry/sorting"

	"github.com/ipfs/go-cid"
	cbornode "github.com/ipfs/go-ipld-cbor"

	"berty.tech/go-ipfs-log/entry"
	"berty.tech/go-ipfs-log/errmsg"
	"berty.tech/go-ipfs-log/io"
)

type FetchOptions struct {
	Length       *int
	Exclude      []iface.IPFSLogEntry
	ProgressChan chan iface.IPFSLogEntry
	Timeout      time.Duration
	Concurrency  int
	SortFn       iface.EntrySortFn
}

func toMultihash(ctx context.Context, services core_iface.CoreAPI, log *IPFSLog) (cid.Cid, error) {
	if log.heads.Len() == 0 {
		return cid.Undef, errmsg.ErrEmptyLogSerialization
	}

	return io.WriteCBOR(ctx, services, log.ToJSON(), nil)
}

func fromMultihash(ctx context.Context, services core_iface.CoreAPI, hash cid.Cid, options *FetchOptions) (*Snapshot, error) {
	result, err := io.ReadCBOR(ctx, services, hash)
	if err != nil {
		return nil, errmsg.ErrCBOROperationFailed.Wrap(err)
	}

	logData := &JSONLog{}
	err = cbornode.DecodeInto(result.RawData(), logData)
	if err != nil {
		return nil, errmsg.ErrCBOROperationFailed.Wrap(err)
	}

	// Use user provided sorting function or the default one
	sortFn := sorting.NoZeroes(sorting.LastWriteWins)
	if options.SortFn != nil {
		sortFn = options.SortFn
	}

	entries := entry.FetchAll(ctx, services, logData.Heads, &iface.FetchOptions{
		Length:       options.Length,
		Exclude:      options.Exclude,
		Concurrency:  options.Concurrency,
		Timeout:      options.Timeout,
		ProgressChan: options.ProgressChan,
	})

	if options.Length != nil && *options.Length > -1 {
		sorting.Sort(sortFn, entries)

		entries = entrySlice(entries, -*options.Length)
	}

	var heads []cid.Cid
	for _, e := range entries {
		for _, h := range logData.Heads {
			if h.String() == e.GetHash().String() {
				heads = append(heads, e.GetHash())
			}
		}
	}

	return &Snapshot{
		ID:     logData.ID,
		Values: entries,
		Heads:  heads,
	}, nil
}

func fromEntryHash(ctx context.Context, services core_iface.CoreAPI, hashes []cid.Cid, options *FetchOptions) ([]iface.IPFSLogEntry, error) {
	if services == nil {
		return nil, errmsg.ErrIPFSNotDefined
	}

	if options == nil {
		return nil, errmsg.ErrFetchOptionsNotDefined
	}

	// Fetch given length, return size at least the given input entries
	length := -1
	if options.Length != nil && *options.Length > -1 {
		length = maxInt(*options.Length, 1)
	}

	all := entry.FetchParallel(ctx, services, hashes, &iface.FetchOptions{
		Length:       options.Length,
		Exclude:      options.Exclude,
		ProgressChan: options.ProgressChan,
		Timeout:      options.Timeout,
		Concurrency:  options.Concurrency,
	})

	sortFn := sorting.NoZeroes(sorting.LastWriteWins)
	if options.SortFn != nil {
		sortFn = options.SortFn
	}

	entries := all
	if length > -1 {
		sorting.Sort(sortFn, entries)
		entries = entrySlice(all, -length)
	}

	return entries, nil
}

func fromJSON(ctx context.Context, services core_iface.CoreAPI, jsonLog *JSONLog, options *iface.FetchOptions) (*Snapshot, error) {
	if services == nil {
		return nil, errmsg.ErrIPFSNotDefined
	}

	if options == nil {
		return nil, errmsg.ErrFetchOptionsNotDefined
	}

	entries := entry.FetchParallel(ctx, services, jsonLog.Heads, &iface.FetchOptions{
		Length:       options.Length,
		ProgressChan: options.ProgressChan,
		Concurrency:  options.Concurrency,
		Timeout:      options.Timeout,
	})

	sorting.Sort(sorting.Compare, entries)

	return &Snapshot{
		ID:     jsonLog.ID,
		Heads:  jsonLog.Heads,
		Values: entries,
	}, nil
}

func fromEntry(ctx context.Context, services core_iface.CoreAPI, sourceEntries []iface.IPFSLogEntry, options *iface.FetchOptions) (*Snapshot, error) {
	if services == nil {
		return nil, errmsg.ErrIPFSNotDefined
	}

	if options == nil {
		return nil, errmsg.ErrFetchOptionsNotDefined
	}

	// Fetch given length, return size at least the given input entries
	length := -1
	if options.Length != nil && *options.Length > -1 {
		length = maxInt(*options.Length, len(sourceEntries))
	}

	// Make sure we pass hashes instead of objects to the fetcher function
	var hashes []cid.Cid
	for _, e := range sourceEntries {
		hashes = append(hashes, e.GetHash())
	}

	// Fetch the entries
	entries := entry.FetchParallel(ctx, services, hashes, &iface.FetchOptions{
		Length:       &length,
		Exclude:      options.Exclude,
		ProgressChan: options.ProgressChan,
		Timeout:      options.Timeout,
		Concurrency:  options.Concurrency,
	})

	// Combine the fetches with the source entries and take only uniques
	combined := append(sourceEntries, entries...)
	combined = append(combined, options.Exclude...)
	uniques := entry.NewOrderedMapFromEntries(combined).Slice()
	sorting.Sort(sorting.Compare, uniques)

	// Cap the result at the right size by taking the last n entries
	var sliced []iface.IPFSLogEntry

	if length > -1 {
		sliced = entrySlice(uniques, -length)
	} else {
		sliced = uniques
	}

	missingSourceEntries := entry.Difference(sliced, sourceEntries)
	result := append(missingSourceEntries, entrySliceRange(sliced, len(missingSourceEntries), len(sliced))...)

	return &Snapshot{
		ID:     result[len(result)-1].GetLogID(),
		Values: result,
	}, nil
}

func entrySlice(entries []iface.IPFSLogEntry, index int) []iface.IPFSLogEntry {
	if len(entries) == 0 || index >= len(entries) {
		return []iface.IPFSLogEntry{}
	}

	if index == 0 || (index < 0 && -index >= len(entries)) {
		return entries
	}

	if index > 0 {
		return entries[index:]
	}

	return entries[(len(entries) + index):]
}

func entrySliceRange(entries []iface.IPFSLogEntry, from int, to int) []iface.IPFSLogEntry {
	if len(entries) == 0 {
		return nil
	}

	if from < 0 {
		from = len(entries) + from
		if from < 0 {
			from = 0
		}
	}

	if to < 0 {
		to = len(entries) + to
	}

	if from >= len(entries) {
		return nil
	}

	if to > len(entries) {
		to = len(entries)
	}

	if from >= to {
		return nil
	}

	if from == to {
		return entries
	}

	return entries[from:to]
}
