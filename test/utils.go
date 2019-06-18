package test // import "berty.tech/go-ipfs-log/test"

import (
	"berty.tech/go-ipfs-log/entry"
	"berty.tech/go-ipfs-log/io"
	bserv "github.com/ipfs/go-blockservice"
	"github.com/ipfs/go-datastore"
	dssync "github.com/ipfs/go-datastore/sync"
	bstore "github.com/ipfs/go-ipfs-blockstore"
	offline "github.com/ipfs/go-ipfs-exchange-offline"
	"github.com/ipfs/go-ipfs/pin"
	"github.com/ipfs/go-merkledag"
	core_iface "github.com/ipfs/interface-go-ipfs-core"
)

type ipfsServices struct {
	dag        core_iface.APIDagService
	blockStore bstore.Blockstore
	ds         datastore.Datastore
	blockserv  bserv.BlockService
	pinner     pin.Pinner
}

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

var _ io.IpfsServices = &ipfsServices{}

func lastEntry(entries []*entry.Entry) *entry.Entry {
	length := len(entries)
	if length > 0 {
		return entries[len(entries)-1]
	}

	return nil
}

func entriesAsStrings(values *entry.OrderedMap) []string {
	var foundEntries []string
	for _, v := range values.Slice() {
		foundEntries = append(foundEntries, string(v.Payload))
	}

	return foundEntries
}

func getLastEntry(omap *entry.OrderedMap) *entry.Entry {
	lastKey := omap.Keys()[len(omap.Keys())-1]

	return omap.UnsafeGet(lastKey)
}

func minInt(a, b int) int {
	if a < b {
		return a
	}
	return b
}

func intPtr(val int) *int {
	return &val
}

var bigLogString = `DONE
└─EOF
  └─entryC10
    └─entryB10
      └─entryA10
    └─entryC9
      └─entryB9
        └─entryA9
      └─entryC8
        └─entryB8
          └─entryA8
        └─entryC7
          └─entryB7
            └─entryA7
          └─entryC6
            └─entryB6
              └─entryA6
            └─entryC5
              └─entryB5
                └─entryA5
              └─entryC4
                └─entryB4
                  └─entryA4
└─3
                └─entryC3
                  └─entryB3
                    └─entryA3
  └─2
                  └─entryC2
                    └─entryB2
                      └─entryA2
    └─1
                    └─entryC1
                      └─entryB1
                        └─entryA1`
