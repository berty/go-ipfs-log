package log

import (
	"github.com/berty/go-ipfs-log/entry"
	"github.com/berty/go-ipfs-log/io"
	"github.com/berty/go-ipfs-log/utils/lamportclock"
	"github.com/ipfs/go-cid"
	cbornode "github.com/ipfs/go-ipld-cbor"
	"github.com/pkg/errors"
	"sort"
	"time"
)

type FetchOptions struct {
	Length int
	Exclude []*entry.Entry
	ProgressChan chan *entry.Entry
	Timeout time.Duration
}


func ToMultihash (services *io.IpfsServices, log *Log) (cid.Cid, error) {
	if len(log.Values()) < 1 {
		return cid.Cid{}, errors.New(`Can't serialize an empty log`)
	}

	return io.WriteCBOR(services, log.ToJSON())
}

func FromMultihash (services *io.IpfsServices, hash cid.Cid, options *FetchOptions) (*Snapshot, error) {
	result, err := io.ReadCBOR(services, hash)
	if err != nil {
		return nil, err
	}

	logData := &JSONLog{}
	err = cbornode.DecodeInto(result.RawData(), logData)
	if err != nil {
		return nil, err
	}

	entries := entry.FetchAll(services, logData.Heads, &entry.FetchOptions{
		Length: options.Length,
		Exclude: options.Exclude,
		ProgressChan: options.ProgressChan,
	})

	// Find latest clock
	var clock *lamportclock.LamportClock
	for _, e := range entries {
		if clock == nil || e.Clock.Time > clock.Time {
			clock = lamportclock.New(e.Clock.ID, e.Clock.Time)
		}
	}

	finalEntries := append(entries[:0:0], entries...)
	sort.SliceStable(finalEntries, func (i, j int) bool {
		ret, err := entry.Compare(finalEntries[i], finalEntries[j])
		if err != nil {
			return false
		}
		return ret > 0
	})

	heads := []*entry.Entry{}
	for _, e := range finalEntries {
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
		ID: logData.ID,
		Values: finalEntries,
		Heads: headsCids,
		Clock: clock,
	}, nil
}

func FromEntryHash (services *io.IpfsServices, hashes []cid.Cid, options *FetchOptions) []*entry.Entry {
	// Fetch given length, return size at least the given input entries
	length := options.Length
	if options.Length > -1 {
		length = maxInt(options.Length, 1)
	}

	entries := entry.FetchAll(services, hashes, &entry.FetchOptions{
		Length: options.Length,
		Exclude: options.Exclude,
		ProgressChan: options.ProgressChan,
	})

	sliced := entries
	if length > -1 {
		entries = entries[:-length]
	}

	return sliced
}

func FromJSON (services *io.IpfsServices, jsonLog JSONLog, options *entry.FetchOptions) *Snapshot {
	entries := entry.FetchAll(services, jsonLog.Heads, &entry.FetchOptions{
		Length: options.Length,
		Exclude: []*entry.Entry{},
		ProgressChan: options.ProgressChan,
		Concurrency: 16,
		Timeout: options.Timeout,
	})

	sort.SliceStable(entries, func (i, j int) bool {
		ret, err := entry.Compare(entries[i], entries[j])
		if err != nil {
			return false
		}
		return ret > 0
	})

	return &Snapshot{
		ID: jsonLog.ID,
		Heads: jsonLog.Heads,
		Values: entries,
	}
}

func FromEntry() {
	
}