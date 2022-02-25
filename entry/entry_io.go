package entry // import "berty.tech/go-ipfs-log/entry"

import (
	"context"

	"github.com/ipfs/go-cid"
	core_iface "github.com/ipfs/interface-go-ipfs-core"

	"berty.tech/go-ipfs-log/iface"
)

type FetchOptions = iface.FetchOptions

// FetchParallel retrieves IPFS log entries.
func FetchParallel(ctx context.Context, ipfs core_iface.CoreAPI, hashes []cid.Cid, options *FetchOptions) []iface.IPFSLogEntry {
	fetcher := NewFetcher(ipfs, options)
	return fetcher.Fetch(ctx, hashes)

	// wg := sync.WaitGroup{}
	// wg.Add(len(hashes))

	// fetchedEntries := make([][]iface.IPFSLogEntry, len(hashes))
	// for i, h := range hashes {
	// 	go func(h cid.Cid, i int) {
	// 		fetchedEntries[i] = fetcher.Fetch(ctx, []cid.Cid{h})
	// 		wg.Done()
	// 	}(h, i)
	// }

	// wg.Wait()

	// entries := []iface.IPFSLogEntry(nil)
	// for i := range hashes {
	// 	entries = append(entries, fetchedEntries[i]...)
	// }

	// return entries
}

// FetchAll gets entries from their CIDs.
func FetchAll(ctx context.Context, ipfs core_iface.CoreAPI, hashes []cid.Cid, options *FetchOptions) []iface.IPFSLogEntry {
	fetcher := NewFetcher(ipfs, options)
	return fetcher.Fetch(ctx, hashes)
}
