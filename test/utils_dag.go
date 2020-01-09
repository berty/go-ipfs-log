package test

import (
	"context"

	"github.com/ipfs/go-cid"
	ipld "github.com/ipfs/go-ipld-format"
	core_iface "github.com/ipfs/interface-go-ipfs-core"
)

type dagWrapper struct {
	dag ipld.DAGService
}

func (d *dagWrapper) Get(ctx context.Context, c cid.Cid) (ipld.Node, error) {
	return d.dag.Get(ctx, c)
}

func (d *dagWrapper) GetMany(ctx context.Context, c []cid.Cid) <-chan *ipld.NodeOption {
	return d.dag.GetMany(ctx, c)
}

func (d *dagWrapper) Add(ctx context.Context, n ipld.Node) error {
	return d.dag.Add(ctx, n)
}

func (d *dagWrapper) AddMany(ctx context.Context, n []ipld.Node) error {
	return d.dag.AddMany(ctx, n)
}

func (d *dagWrapper) Remove(ctx context.Context, c cid.Cid) error {
	return d.dag.Remove(ctx, c)
}

func (d *dagWrapper) RemoveMany(ctx context.Context, c []cid.Cid) error {
	return d.dag.RemoveMany(ctx, c)
}

func (d *dagWrapper) Pinning() ipld.NodeAdder {
	return d.dag
}

var _ core_iface.APIDagService = &dagWrapper{}
