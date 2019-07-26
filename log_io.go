package ipfslog // import "berty.tech/go-ipfs-log"

import (
	"context"
	"errors"
	"time"

	"berty.tech/go-ipfs-log/entry/sorting"

	"berty.tech/go-ipfs-log/entry"
	"berty.tech/go-ipfs-log/errmsg"
	"berty.tech/go-ipfs-log/io"
	"github.com/ipfs/go-cid"
	cbornode "github.com/ipfs/go-ipld-cbor"
)

type FetchOptions struct {
	Length       *int
	Exclude      []*entry.Entry
	ProgressChan chan *entry.Entry
	Timeout      time.Duration
}

func toMultihash(ctx context.Context, services io.IpfsServices, log *Log) (cid.Cid, error) {
	if log.Values().Len() < 1 {
		return cid.Cid{}, errors.New(`can't serialize an empty log`)
	}

	return io.WriteCBOR(ctx, services, log.ToJSON())
}

func fromMultihash(ctx context.Context, services io.IpfsServices, hash cid.Cid, options *FetchOptions) (*Snapshot, error) {
	result, err := io.ReadCBOR(ctx, services, hash)
	if err != nil {
		return nil, err
	}

	logData := &JSONLog{}
	err = cbornode.DecodeInto(result.RawData(), logData)
	if err != nil {
		return nil, err
	}

	entries := entry.FetchAll(ctx, services, logData.Heads, &entry.FetchOptions{
		Length:       options.Length,
		Exclude:      options.Exclude,
		ProgressChan: options.ProgressChan,
	})

	// Find latest clock
	var clock *entry.LamportClock
	for _, e := range entries {
		if clock == nil || e.Clock.Time > clock.Time {
			clock = entry.NewLamportClock(e.Clock.ID, e.Clock.Time)
		}
	}

	sorting.Sort(sorting.Compare, entries)

	heads := []*entry.Entry{}
	for _, e := range entries {
		for _, h := range logData.Heads {
			if h.String() == e.Hash.String() {
				heads = append(heads, e)
			}
		}
	}

	headsCids := []cid.Cid{}
	for _, head := range heads {
		headsCids = append(headsCids, head.Hash)
	}

	return &Snapshot{
		ID:     logData.ID,
		Values: entries,
		Heads:  headsCids,
		Clock:  clock,
	}, nil
}

func fromEntryHash(ctx context.Context, services io.IpfsServices, hashes []cid.Cid, options *FetchOptions) ([]*entry.Entry, error) {
	if services == nil {
		return nil, errmsg.IPFSNotDefined
	}

	if options == nil {
		return nil, errmsg.FetchOptionsNotDefined
	}

	// Fetch given length, return size at least the given input entries
	length := -1
	if options.Length != nil && *options.Length > -1 {
		length = maxInt(*options.Length, 1)
	}

	entries := entry.FetchParallel(ctx, services, hashes, &entry.FetchOptions{
		Length:       options.Length,
		Exclude:      options.Exclude,
		ProgressChan: options.ProgressChan,
	})

	sliced := entries
	if length > -1 {
		sliced = entrySlice(sliced, -length)
	}

	return sliced, nil
}

func fromJSON(ctx context.Context, services io.IpfsServices, jsonLog *JSONLog, options *entry.FetchOptions) (*Snapshot, error) {
	if services == nil {
		return nil, errmsg.IPFSNotDefined
	}

	if options == nil {
		return nil, errmsg.FetchOptionsNotDefined
	}

	entries := entry.FetchParallel(ctx, services, jsonLog.Heads, &entry.FetchOptions{
		Length:       options.Length,
		Exclude:      []*entry.Entry{},
		ProgressChan: options.ProgressChan,
		Concurrency:  16,
		Timeout:      options.Timeout,
	})

	sorting.Sort(sorting.Compare, entries)

	return &Snapshot{
		ID:     jsonLog.ID,
		Heads:  jsonLog.Heads,
		Values: entries,
	}, nil
}

func fromEntry(ctx context.Context, services io.IpfsServices, sourceEntries []*entry.Entry, options *entry.FetchOptions) (*Snapshot, error) {
	if services == nil {
		return nil, errmsg.IPFSNotDefined
	}

	if options == nil {
		return nil, errmsg.FetchOptionsNotDefined
	}

	// Fetch given length, return size at least the given input entries
	length := -1
	if options.Length != nil && *options.Length > -1 {
		length = maxInt(*options.Length, len(sourceEntries))
	}

	// Make sure we pass hashes instead of objects to the fetcher function
	hashes := []cid.Cid{}
	for _, e := range sourceEntries {
		hashes = append(hashes, e.Hash)
	}

	// Fetch the entries
	entries := entry.FetchParallel(ctx, services, hashes, &entry.FetchOptions{
		Length:       &length,
		Exclude:      options.Exclude,
		ProgressChan: options.ProgressChan,
	})

	// Combine the fetches with the source entries and take only uniques
	combined := append(sourceEntries, entries...)
	uniques := entry.NewOrderedMapFromEntries(combined).Slice()
	sorting.Sort(sorting.Compare, uniques)

	// Cap the result at the right size by taking the last n entries
	var sliced []*entry.Entry

	if length > -1 {
		sliced = entrySlice(uniques, -length)
	} else {
		sliced = uniques
	}

	missingSourceEntries := entry.Difference(sliced, sourceEntries)
	result := append(missingSourceEntries, entrySliceRange(sliced, len(missingSourceEntries), len(sliced))...)

	return &Snapshot{
		ID:     result[len(result)-1].LogID,
		Values: result,
	}, nil
}

func entrySlice(entries []*entry.Entry, index int) []*entry.Entry {
	if len(entries) == 0 || index >= len(entries) {
		return []*entry.Entry{}
	}

	if index == 0 || (index < 0 && -index >= len(entries)) {
		return entries
	}

	if index > 0 {
		return entries[index:]
	}

	return entries[(len(entries) + index):]
}

func entrySliceRange(entries []*entry.Entry, from int, to int) []*entry.Entry {
	if len(entries) == 0 {
		return []*entry.Entry{}
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
		return []*entry.Entry{}
	}

	if to > len(entries) {
		to = len(entries)
	}

	if from >= to {
		return []*entry.Entry{}
	}

	if from == to {
		return entries
	}

	return entries[from:to]
}
