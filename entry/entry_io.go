package entry

import (
	"context"
	"fmt"

	"berty.tech/go-ipfs-log/identity"
	"berty.tech/go-ipfs-log/io"
	"github.com/ipfs/go-cid"
	"github.com/pkg/errors"
)

// FetchParallel retrieves IPFS log entries.
func FetchParallel(ctx context.Context, ipfs io.IpfsServices, hashes []cid.Cid, options *FetchOptions) []*Entry {
	var entries []*Entry

	for _, h := range hashes {
		entries = append(entries, FetchAll(ctx, ipfs, []cid.Cid{h}, options)...)
	}

	// TODO: parallelize things

	// Flatten the results and get unique vals
	return NewOrderedMapFromEntries(entries).Slice()
}

// FetchAll gets entries from their CIDs.
func FetchAll(ctx context.Context, ipfs io.IpfsServices, hashes []cid.Cid, options *FetchOptions) []*Entry {
	var result []*Entry
	cache := NewOrderedMap()
	loadingQueue := append(hashes[:0:0], hashes...)
	length := -1
	if options.Length != nil {
		length = *options.Length
	}

	addToResults := func(entry *Entry) {
		if entry.IsValid() {
			loadingQueue = append(loadingQueue, entry.Next...)
			result = append(result, entry)
			cache.Set(entry.Hash.String(), entry)

			if options.ProgressChan != nil {
				options.ProgressChan <- entry
			}
		}
	}

	for _, e := range options.Exclude {
		if e.IsValid() {
			result = append(result, e)
			cache.Set(e.GetHash().String(), e)
		}
	}

	shouldFetchMore := func() bool {
		return len(loadingQueue) > 0 && (len(result) < length || length <= 0)
	}

	fetchEntry := func() {
		hash := loadingQueue[0]
		loadingQueue = loadingQueue[1:]

		if _, ok := cache.Get(hash.String()); ok {
			return
		}

		entry, err := fromMultihash(ctx, ipfs, hash, options.Provider)
		if err != nil {
			fmt.Printf("unable to fetch entry %s, %+v\n", hash, err)
			return
		}

		entry.Hash = hash

		if entry.IsValid() {
			addToResults(entry)
		}
	}

	for shouldFetchMore() {
		fetchEntry()
	}

	return result
}

// fromMultihash creates an Entry from a hash.
func fromMultihash(ctx context.Context, ipfs io.IpfsServices, hash cid.Cid, provider identity.Provider) (*Entry, error) {
	if ipfs == nil {
		return nil, errors.New("ipfs instance not defined")
	}

	result, err := io.ReadCBOR(ctx, ipfs, hash)
	if err != nil {
		return nil, err
	}

	obj := &CborEntry{}
	err = cbornode.DecodeInto(result.RawData(), obj)
	if err != nil {
		return nil, err
	}

	obj.Hash = hash

	entry, err := obj.ToEntry(provider)
	if err != nil {
		return nil, err
	}

	return entry, nil
}
