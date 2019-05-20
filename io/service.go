package io

import (
	bserv "github.com/ipfs/go-blockservice"
	ds "github.com/ipfs/go-datastore"
	dssync "github.com/ipfs/go-datastore/sync"
	bstore "github.com/ipfs/go-ipfs-blockstore"
	offline "github.com/ipfs/go-ipfs-exchange-offline"
	"github.com/ipfs/go-ipfs/pin"
	ipld "github.com/ipfs/go-ipld-format"
	merkledag "github.com/ipfs/go-merkledag"
)

type IpfsServices struct {
	DAG        ipld.DAGService
	BlockStore bstore.Blockstore
	DB         ds.Datastore
	Blockserv  bserv.BlockService
	Pinner     pin.Pinner
}

func NewMemoryServices() *IpfsServices {
	dataStore := ds.NewMapDatastore()
	db := dssync.MutexWrap(dataStore)
	bs := bstore.NewBlockstore(db)
	blockserv := bserv.New(bs, offline.Exchange(bs))
	dag := merkledag.NewDAGService(blockserv)
	pinner := pin.NewPinner(db, dag, dag)

	// var pinning pin.Pinner = pin.NewPinner()
	// var blockstore bstore.GCBlockstore = bstore.NewBlockstore()
	return &IpfsServices{
		DAG:        dag,
		BlockStore: bs,
		DB:         db,
		Blockserv:  blockserv,
		Pinner:     pinner,
	}
}
