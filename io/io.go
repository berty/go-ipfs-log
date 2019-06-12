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

func WriteCBOR(ipfs *IpfsServices, obj interface{}) (cid.Cid, error) {
	cborNode, err := cbornode.WrapObject(obj, math.MaxUint64, -1)
	if err != nil {
		return cid.Cid{}, err
	}

	if debug {
		fmt.Printf("\nStr of cbor: %x\n", cborNode.RawData())
	}

	err = ipfs.DAG.Add(context.Background(), cborNode)
	if err != nil {
		return cid.Cid{}, err
	}

	return cborNode.Cid(), nil
}

func ReadCBOR(ipfs *IpfsServices, contentIdentifier cid.Cid) (format.Node, error) {
	return ipfs.DAG.Get(context.Background(), contentIdentifier)
}
