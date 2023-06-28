package entry // import "berty.tech/go-ipfs-log/entry"

import (
	"context"

	"github.com/ipfs/go-cid"
	format "github.com/ipfs/go-ipld-format"

	"berty.tech/go-ipfs-log/iface"
)

type FetchOptions = iface.FetchOptions

// FetchParallel has the same comportement than FetchAll, we keep it for retrop
// compatibility purpose
func FetchParallel(ctx context.Context, dag format.DAGService, hashes []cid.Cid, options *FetchOptions) []iface.IPFSLogEntry {
	fetcher := NewFetcher(dag, options)
	return fetcher.Fetch(ctx, hashes)
}

// FetchAll gets entries from their CIDs.
func FetchAll(ctx context.Context, dag format.DAGService, hashes []cid.Cid, options *FetchOptions) []iface.IPFSLogEntry {
	fetcher := NewFetcher(dag, options)
	return fetcher.Fetch(ctx, hashes)
}
