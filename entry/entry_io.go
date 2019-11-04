package entry // import "berty.tech/go-ipfs-log/entry"

import (
	"berty.tech/go-ipfs-log/iface"
	"berty.tech/go-ipfs-log/io"
	"context"
	"fmt"
	"github.com/ipfs/go-cid"
)

type FetchOptions = iface.FetchOptions

// FetchParallel retrieves IPFS log entries.
func FetchParallel(ctx context.Context, ipfs io.IpfsServices, hashes []cid.Cid, options *FetchOptions) []iface.IPFSLogEntry {
	var entries []iface.IPFSLogEntry

	for _, h := range hashes {
		entries = append(entries, FetchAll(ctx, ipfs, []cid.Cid{h}, options)...)
	}

	// TODO: parallelize things

	// Flatten the results and get unique vals
	return NewOrderedMapFromEntries(entries).Slice()
}

// FetchAll gets entries from their CIDs.
func FetchAll(ctx context.Context, ipfs io.IpfsServices, hashes []cid.Cid, options *FetchOptions) []iface.IPFSLogEntry {
	var result []iface.IPFSLogEntry
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
