package ipfslog // import "berty.tech/go-ipfs-log"

import (
	"berty.tech/go-ipfs-log/iface"
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
	Exclude      []iface.IPFSLogEntry
	ProgressChan chan iface.IPFSLogEntry
	Timeout      time.Duration
}

func toMultihash(ctx context.Context, services io.IpfsServices, log *IPFSLog) (cid.Cid, error) {
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

	entries := entry.FetchAll(ctx, services, logData.Heads, &iface.FetchOptions{
		Length:       options.Length,
		Exclude:      options.Exclude,
		ProgressChan: options.ProgressChan,
	})

	// Find latest clock
	var clock iface.IPFSLogLamportClock
	for _, e := range entries {
		if clock == nil || e.GetClock().GetTime() > clock.GetTime() {
			clock = entry.NewLamportClock(e.GetClock().GetID(), e.GetClock().GetTime())
		}
	}

	sorting.Sort(sorting.Compare, entries)

	var heads []iface.IPFSLogEntry
	for _, e := range entries {
		for _, h := range logData.Heads {
			if h.String() == e.GetHash().String() {
				heads = append(heads, e)
			}
		}
	}

	var headsCids []cid.Cid
	for _, head := range heads {
		headsCids = append(headsCids, head.GetHash())
	}

	return &Snapshot{
		ID:     logData.ID,
		Values: entries,
		Heads:  headsCids,
		Clock:  clock,
	}, nil
}

func fromEntryHash(ctx context.Context, services io.IpfsServices, hashes []cid.Cid, options *FetchOptions) ([]iface.IPFSLogEntry, error) {
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

	entries := entry.FetchParallel(ctx, services, hashes, &iface.FetchOptions{
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

func fromJSON(ctx context.Context, services io.IpfsServices, jsonLog *JSONLog, options *iface.FetchOptions) (*Snapshot, error) {
	if services == nil {
		return nil, errmsg.IPFSNotDefined
	}

	if options == nil {
		return nil, errmsg.FetchOptionsNotDefined
	}

	entries := entry.FetchParallel(ctx, services, jsonLog.Heads, &iface.FetchOptions{
		Length:       options.Length,
		Exclude:      []iface.IPFSLogEntry{},
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

func fromEntry(ctx context.Context, services io.IpfsServices, sourceEntries []iface.IPFSLogEntry, options *iface.FetchOptions) (*Snapshot, error) {
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
	var hashes []cid.Cid
	for _, e := range sourceEntries {
		hashes = append(hashes, e.GetHash())
	}

	// Fetch the entries
	entries := entry.FetchParallel(ctx, services, hashes, &iface.FetchOptions{
		Length:       &length,
		Exclude:      options.Exclude,
		ProgressChan: options.ProgressChan,
	})

	// Combine the fetches with the source entries and take only uniques
	combined := append(sourceEntries, entries...)
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
