package entry // import "berty.tech/go-ipfs-log/entry"

import (
	"context"
	"sync"
	"time"

	"github.com/ipfs/go-cid"

	"berty.tech/go-ipfs-log/iface"
	"berty.tech/go-ipfs-log/io"
)

type FetchOptions = iface.FetchOptions

// FetchParallel retrieves IPFS log entries.
func FetchParallel(ctx context.Context, ipfs io.IpfsServices, hashes []cid.Cid, options *FetchOptions) []iface.IPFSLogEntry {
	var (
		entries        = []iface.IPFSLogEntry(nil)
		fetchedEntries = make([][]iface.IPFSLogEntry, len(hashes))
		wg             = sync.WaitGroup{}
	)

	wg.Add(len(hashes))

	for i, h := range hashes {
		go func(h cid.Cid, i int) {
			defer wg.Done()

			fetchedEntries[i] = FetchAll(ctx, ipfs, []cid.Cid{h}, options)
		}(h, i)
	}

	wg.Wait()

	for i := range hashes {
		entries = append(entries, fetchedEntries[i]...)
	}

	return entries
}

// FetchAll gets entries from their CIDs.
func FetchAll(ctx context.Context, ipfs io.IpfsServices, hashes []cid.Cid, options *FetchOptions) []iface.IPFSLogEntry {
	var (
		lock         = sync.Mutex{}
		result       = []iface.IPFSLogEntry(nil)
		cache        = map[cid.Cid]bool{}
		loadingCache = map[cid.Cid]bool{}
		loadingQueue = map[int][]cid.Cid{0: hashes}
		running      = 0 // keep track of how many entries are being fetched at any time
		maxClock     = 0 // keep track of the latest clock time during load
		minClock     = 0 // keep track of the minimum clock time during load
		concurrency  = 1
		done         = make(chan bool)
		length       = -1
	)

	if options.Length != nil {
		length = *options.Length
	}

	if options.Concurrency > concurrency {
		concurrency = options.Concurrency
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Add a multihash to the loading queue
	addToLoadingQueue := func(e cid.Cid, idx int) {
		lock.Lock()
		defer lock.Unlock()

		if _, ok := loadingCache[e]; ok {
			return
		}

		loadingCache[e] = true

		for _, otherE := range loadingQueue[idx] {
			if otherE.Equals(e) {
				return
			}
		}

		loadingQueue[idx] = append(loadingQueue[idx], e)
	}

	// Get the next items to process from the loading queue
	getNextFromQueue := func(length int) []cid.Cid {
		lock.Lock()
		defer lock.Unlock()

		if length == 0 {
			length = 1
		}

		res := []cid.Cid(nil)

		for key := range loadingQueue {
			nextItems := loadingQueue[key]
			for len(nextItems) > 0 && len(res) < length {
				h := nextItems[0]
				nextItems = nextItems[1:]

				res = append(res, h)
			}

			loadingQueue[key] = nextItems

			if len(nextItems) == 0 {
				delete(loadingQueue, key)
			}
		}

		return res
	}

	// Fetch one entry and add it to the results
	fetchEntry := func(hash cid.Cid) {
		if !hash.Defined() {
			return
		}

		if _, ok := cache[hash]; ok {
			return
		}

		addToResults := func(entry iface.IPFSLogEntry) {
			if !entry.IsValid() {
				return
			}

			ts := entry.GetClock().GetTime()

			// Update min/max clocks
			if maxClock < ts {
				maxClock = ts
			}

			if len(result) > 0 {
				if ts := result[len(result)-1].GetClock().GetTime(); ts < minClock {
					minClock = ts
				}
			} else {
				minClock = maxClock
			}

			isLater := len(result) >= length && ts >= minClock
			// const calculateIndex = (idx) => maxClock - ts + ((idx + 1) * idx)

			// Add the entry to the results if
			// 1) we're fetching all entries
			// 2) results is not filled yet
			// the clock of the entry is later than current known minimum clock time
			if length < 0 || len(result) < length || isLater {
				result = append(result, entry)
				cache[hash] = true

				if options.ProgressChan != nil {
					options.ProgressChan <- entry
				}

			}

			if length < 0 {
				// If we're fetching all entries (length === -1), adds nexts and refs to the queue
				for i, h := range entry.GetNext() {
					addToLoadingQueue(h, i)
				}

				for i, h := range entry.GetRefs() {
					addToLoadingQueue(h, i)
				}
			} else {
				// If we're fetching entries up to certain length,
				// fetch the next if result is filled up, to make sure we "check"
				// the next entry if its clock is later than what we have in the result
				if _, ok := cache[entry.GetHash()]; len(result) < length || ts > minClock || ts == minClock && !ok {
					for _, h := range entry.GetNext() {
						addToLoadingQueue(h, maxClock-ts)
					}
				}
				if len(result)+len(entry.GetRefs()) <= length {
					for i, h := range entry.GetRefs() {
						addToLoadingQueue(h, maxClock-ts+((i+1)*i))
					}
				}
			}
		}

		// Load the entry
		entry, err := FromMultihash(ctx, ipfs, hash, options.Provider)
		if err != nil {
			// TODO: log
			return
		}

		// Add it to the results
		addToResults(entry)
	}

	// Add entries to exclude from processing to the cache before we start
	// Add entries that we don't need to fetch to the "cache"
	for _, e := range options.Exclude {
		cache[e.GetHash()] = true
	}

	loadingQueueHasItems := func() bool {
		for _, s := range loadingQueue {
			if len(s) > 0 {
				return true
			}
		}

		return false
	}

	go func() {
		// Does the loading queue have more to process?
		for loadingQueueHasItems() {
			if running < concurrency {
				nexts := getNextFromQueue(concurrency)
				running += len(nexts)
				for _, n := range nexts {
					fetchEntry(n)
				}

				running -= len(nexts)
			}
		}
		done <- true
	}()

	// Resolve the promise after a timeout (if given) in order to
	// not get stuck loading a block that is unreachable
	if options.Timeout != 0 {
		select {
		case <-time.After(options.Timeout):
			return result
		case <-done:
			return result
		}
	}

	<-done

	return result
}
