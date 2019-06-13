package entry // import "berty.tech/go-ipfs-log/entry"

import (
	"context"
	"fmt"
	"time"

	"berty.tech/go-ipfs-log/identityprovider"
	"berty.tech/go-ipfs-log/io"
	cid "github.com/ipfs/go-cid"
)

type FetchOptions struct {
	Length       *int
	Exclude      []*Entry
	Concurrency  int
	Timeout      time.Duration
	ProgressChan chan *Entry
	Provider     identityprovider.Interface
}

func FetchParallel(ipfs io.IpfsServices, hashes []cid.Cid, options *FetchOptions) []*Entry {
	var entries []*Entry

	for _, h := range hashes {
		entries = append(entries, FetchAll(ipfs, []cid.Cid{h}, options)...)
	}

	// TODO: parallelize things

	// Flatten the results and get unique vals
	return NewOrderedMapFromEntries(entries).Slice()
}

func FetchAll(ipfs io.IpfsServices, hashes []cid.Cid, options *FetchOptions) []*Entry {
	result := []*Entry{}
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
			cache.Set(e.Hash.String(), e)
		}
	}

	shouldFetchMore := func() bool {
		return len(loadingQueue) > 0 && (len(result) < length || length <= 0)
	}

	fetchEntry := func() {
		var loadingQueueStrings []string
		for _, c := range loadingQueue {
			loadingQueueStrings = append(loadingQueueStrings, c.String())
		}

		hash := loadingQueue[0]
		loadingQueue = loadingQueue[1:]

		if _, ok := cache.Get(hash.String()); ok {
			return
		}

		ctx := context.Background()

		if options.Timeout != 0 {
			ctx, _ = context.WithTimeout(ctx, options.Timeout)
		}

		entry, err := FromMultihash(ipfs, hash, options.Provider)
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
