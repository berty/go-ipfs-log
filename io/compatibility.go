package io

import (
	"context"

	"berty.tech/go-ipfs-log/entry"
	"berty.tech/go-ipfs-log/iface"
	"berty.tech/go-ipfs-log/io/cbor"
	"github.com/ipfs/go-cid"
	format "github.com/ipfs/go-ipld-format"
)

type CBOROptions = cbor.Options

func ReadCBOR(ctx context.Context, getter format.NodeGetter, c cid.Cid) (format.Node, error) {
	io, err := cbor.IO(&entry.Entry{}, &entry.LamportClock{})
	if err != nil {
		return nil, err
	}

	return io.Read(ctx, getter, c)
}

func WriteCBOR(ctx context.Context, adder format.NodeAdder, obj interface{}, opts *iface.WriteOpts) (cid.Cid, error) {
	io, err := cbor.IO(&entry.Entry{}, &entry.LamportClock{})
	if err != nil {
		return cid.Undef, err
	}

	return io.Write(ctx, adder, opts, obj)
}

func CBOR() *cbor.IOCbor {
	io, err := cbor.IO(&entry.Entry{}, &entry.LamportClock{})
	if err != nil {
		panic(err)
	}

	return io
}
