// Package io defines helpers used within IPFS Log and OrbitDB.
package io // import "berty.tech/go-ipfs-log/io"

import (
	"context"
	"fmt"
	"math"

	"github.com/ipfs/go-cid"
	cbornode "github.com/ipfs/go-ipld-cbor"
	format "github.com/ipfs/go-ipld-format"
	"github.com/ipfs/interface-go-ipfs-core/path"
)

var debug = false

func SetDebug(val bool) {
	debug = val
}

type WriteOpts struct {
	Pin bool
}

// WriteCBOR writes a CBOR representation of a given object in IPFS' DAG.
func WriteCBOR(ctx context.Context, ipfs IpfsServices, obj interface{}, opts *WriteOpts) (cid.Cid, error) {
	if opts == nil {
		opts = &WriteOpts{}
	}

	cborNode, err := cbornode.WrapObject(obj, math.MaxUint64, -1)
	if err != nil {
		return cid.Undef, err
	}

	if debug {
		fmt.Printf("\nStr of cbor: %x\n", cborNode.RawData())
	}

	err = ipfs.Dag().Add(ctx, cborNode)
	if err != nil {
		return cid.Undef, err
	}

	if opts.Pin {
		if err = ipfs.Pin().Add(ctx, path.IpfsPath(cborNode.Cid())); err != nil {
			return cid.Undef, err
		}
	}

	return cborNode.Cid(), nil
}

// ReadCBOR reads a CBOR representation of a given object from IPFS' DAG.
func ReadCBOR(ctx context.Context, ipfs IpfsServices, contentIdentifier cid.Cid) (format.Node, error) {
	return ipfs.Dag().Get(ctx, contentIdentifier)
}
