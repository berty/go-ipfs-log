package test

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/berty/go-ipfs-log/entry"
	idp "github.com/berty/go-ipfs-log/identityprovider"
	io "github.com/berty/go-ipfs-log/io"
	ks "github.com/berty/go-ipfs-log/keystore"
	"github.com/berty/go-ipfs-log/log"
	cid "github.com/ipfs/go-cid"
	ds "github.com/ipfs/go-datastore"
	dssync "github.com/ipfs/go-datastore/sync"

	. "github.com/smartystreets/goconvey/convey"
)

func TestPersistency(t *testing.T) {
	_, cancel := context.WithTimeout(context.Background(), time.Second*10)
	defer cancel()

	ipfs := io.NewMemoryServices()

	datastore := dssync.MutexWrap(ds.NewMapDatastore())
	keystore, err := ks.NewKeystore(datastore)
	if err != nil {
		panic(err)
	}

	idProvider := idp.NewOrbitDBIdentityProvider(keystore)

	var identities []*idp.Identity

	for i := 0; i < 4; i++ {
		identity, err := idProvider.GetID(fmt.Sprintf("User%d", i))
		if err != nil {
			panic(err)
		}

		identities = append(identities, identity)
	}

	Convey("Entry - Persistency", t, FailureHalts, func(c C) {
		c.Convey("log with one entry", FailureHalts, func(c C) {
			log1 := log.NewLog(ipfs, identities[0], &log.NewLogOptions{ID: "X"})
			e, err := log1.Append([]byte("K1K00"), 1)
			c.So(err, ShouldBeNil)

			hash := e.Hash
			res := entry.FetchAll(ipfs, []cid.Cid{hash}, &entry.FetchOptions{})
			c.So(len(res), ShouldEqual, 1)
		})
	})
}
