package entry

import (
	"context"
	"fmt"
	"github.com/berty/go-ipfs-log/identityprovider"
	"github.com/berty/go-ipfs-log/io"
	"github.com/ipfs/go-cid"
	"time"
)

type FetchOptions struct {
	Length       *int
	Exclude      []*Entry
	Concurrency  int
	Timeout      time.Duration
	ProgressChan chan *Entry
	Provider     identityprovider.Interface
}

func FetchAll(ipfs *io.IpfsServices, hashes []cid.Cid, options *FetchOptions) []*Entry {
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
