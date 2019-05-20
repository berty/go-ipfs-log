package entry

import (
	"context"
	"github.com/berty/go-ipfs-log/io"
	"github.com/ipfs/go-cid"
	"time"
)

type FetchOptions struct {
	Length int
	Exclude []*Entry
	Concurrency int
	Timeout time.Duration
	ProgressChan chan *Entry
}

func FetchAll (ipfs *io.IpfsServices, hashes []cid.Cid, options *FetchOptions) []*Entry {
	result := []*Entry{}
	cache := map[string]*Entry{}
	loadingQueue := append(hashes[:0:0], hashes...)

	addToResults := func (entry *Entry) {
		if entry.IsValid() {
			loadingQueue = append(loadingQueue, entry.Next...)
			result = append(result, entry)
			cache[entry.Hash.String()] = entry

			if options.ProgressChan != nil {
				options.ProgressChan <- entry
			}
		}
	}

	for _, e := range options.Exclude {
		if e.IsValid() {
			result = append(result, e)
			cache[e.Hash.String()] = e
		}
	}

	shouldFetchMore := func () bool {
		return len(loadingQueue) > 0 && (len(result) < options.Length || options.Length < 0)
	}

	fetchEntry := func () {
		hash := loadingQueue[0]
		loadingQueue = loadingQueue[1:]

		if _, ok := cache[hash.String()]; ok {
			return
		}

		ctx := context.Background()

		if options.Timeout != 0 {
			ctx, _ = context.WithTimeout(ctx, options.Timeout)
		}

		entry, err := FromMultihash(ipfs, hash)
		if err != nil {
			return
		}

		if entry.IsValid() {
			addToResults(entry)
		}
	}

	for shouldFetchMore() {
		fetchEntry()
	}

	return result
}
