package io

import (
	"context"
	"github.com/ipfs/go-cid"
	"github.com/ipfs/go-ipld-cbor"
	format "github.com/ipfs/go-ipld-format"
	"math"
)

func WriteCBOR(ipfs *IpfsServices, obj interface{}) (cid.Cid, error) {
	cborNode, err := cbornode.WrapObject(obj, math.MaxUint64, -1)
	if err != nil {
		return cid.Cid{}, err
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

