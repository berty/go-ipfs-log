// Package io defines helpers used within IPFS Log and OrbitDB.
package io // import "berty.tech/go-ipfs-log/io"

import (
	"context"
	"fmt"
	"math"

	cid "github.com/ipfs/go-cid"
	cbornode "github.com/ipfs/go-ipld-cbor"
	format "github.com/ipfs/go-ipld-format"
)

var debug = false

func SetDebug(val bool) {
	debug = val
}

// WriteCBOR writes a CBOR representation of a given object in IPFS' DAG.
func WriteCBOR(ipfs IpfsServices, obj interface{}) (cid.Cid, error) {
	cborNode, err := cbornode.WrapObject(obj, math.MaxUint64, -1)
	if err != nil {
		return cid.Cid{}, err
	}

	if debug {
		fmt.Printf("\nStr of cbor: %x\n", cborNode.RawData())
	}

	err = ipfs.Dag().Add(context.Background(), cborNode)
	if err != nil {
		return cid.Cid{}, err
	}

	return cborNode.Cid(), nil
}

// ReadCBOR reads a CBOR representation of a given object from IPFS' DAG.
func ReadCBOR(ipfs IpfsServices, contentIdentifier cid.Cid) (format.Node, error) {
	return ipfs.Dag().Get(context.Background(), contentIdentifier)
}
