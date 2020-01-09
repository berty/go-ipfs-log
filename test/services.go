package test

import (
	"berty.tech/go-ipfs-log/io"
	bserv "github.com/ipfs/go-blockservice"
	"github.com/ipfs/go-datastore"
	dssync "github.com/ipfs/go-datastore/sync"
	bstore "github.com/ipfs/go-ipfs-blockstore"
	offline "github.com/ipfs/go-ipfs-exchange-offline"
	pin "github.com/ipfs/go-ipfs-pinner"
	"github.com/ipfs/go-merkledag"
	core_iface "github.com/ipfs/interface-go-ipfs-core"
)

func NewMemoryServices() io.IpfsServices {
	dataStore := datastore.NewMapDatastore()
	db := dssync.MutexWrap(dataStore)
	bs := bstore.NewBlockstore(db)
	blockserv := bserv.New(bs, offline.Exchange(bs))
	dag := merkledag.NewDAGService(blockserv)
	pinner := pin.NewPinner(db, dag, dag)

	return &ipfsServices{
		dag:        &dagWrapper{dag: dag},
		blockStore: bs,
		ds:         db,
		blockserv:  blockserv,
		pinner:     pinner,
	}
}

type ipfsServices struct {
	dag        core_iface.APIDagService
	blockStore bstore.Blockstore
	ds         datastore.Datastore
	blockserv  bserv.BlockService
	pinner     pin.Pinner
}

func (i *ipfsServices) Dag() core_iface.APIDagService {
	return i.dag
}

var _ io.IpfsServices = &ipfsServices{}
