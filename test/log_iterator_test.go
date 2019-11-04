package test

import (
	"berty.tech/go-ipfs-log/iface"
	"berty.tech/go-ipfs-log/test/logcreator"
	"context"
	"fmt"
	"github.com/ipfs/go-cid"
	. "github.com/smartystreets/goconvey/convey"
	"testing"

	ipfslog "berty.tech/go-ipfs-log"

	idp "berty.tech/go-ipfs-log/identityprovider"
	ks "berty.tech/go-ipfs-log/keystore"
	dssync "github.com/ipfs/go-datastore/sync"
	ipfsCore "github.com/ipfs/go-ipfs/core"
	"github.com/ipfs/go-ipfs/core/coreapi"
)

func TestLogIterator(t *testing.T) {
	ctx := context.Background()

	Convey("IPFSLog - Iterator", t, FailureHalts, func(c C) {
		datastore := dssync.MutexWrap(NewIdentityDataStore())
		keystore, err := ks.NewKeystore(datastore)
		if err != nil {
			panic(err)
		}

		var identities [3]*idp.Identity

		for i, char := range []rune{'A', 'B', 'C'} {
			identity, err := idp.CreateIdentity(&idp.CreateIdentityOptions{
				Keystore: keystore,
				ID:       fmt.Sprintf("user%c", char),
				Type:     "orbitdb",
			})

			if err != nil {
				panic(err)
			}

			identities[i] = identity
		}

		core, err := ipfsCore.NewNode(ctx, &ipfsCore.BuildCfg{})
		c.So(err, ShouldBeNil)

		ipfs, err := coreapi.NewCoreAPI(core)

		log1, err := ipfslog.NewLog(ipfs, identities[0], &ipfslog.LogOptions{ ID: "X" })
		c.So(err, ShouldBeNil)

		for i := 0; i <= 100; i++ {
			_, err := log1.Append(ctx, []byte(fmt.Sprintf("entry%d", i)), 1)
			c.So(err, ShouldBeNil)
		}

		c.Convey("Basic iterator functionality", FailureHalts, func(c C) {
			c.Convey("returns length with lte and amount", FailureHalts, func(c C) {
				amount := 10
				resultChan := make(chan iface.IPFSLogEntry, 110)

				parsedCid, err := cid.Parse("bafyreigrbir6zv5cii7y7tw3z5fegsfujedcagd4ws45gzbtig52j4k2my")
				c.So(err, ShouldBeNil)

				err = log1.Iterator(&ipfslog.IteratorOptions{
					LTE: []cid.Cid{parsedCid},
					Amount: &amount,
				}, resultChan)

				c.So(err, ShouldBeNil)
				c.So(len(resultChan), ShouldEqual, amount)
			})

			c.Convey("returns entries with lte and amount", FailureHalts, func(c C) {
				amount := 10
				resultChan := make(chan iface.IPFSLogEntry, 110)

				parsedCid, err := cid.Parse("bafyreigrbir6zv5cii7y7tw3z5fegsfujedcagd4ws45gzbtig52j4k2my")
				c.So(err, ShouldBeNil)

				err = log1.Iterator(&ipfslog.IteratorOptions{
					LTE: []cid.Cid{parsedCid},
					Amount: &amount,
				}, resultChan)

				c.So(err, ShouldBeNil)

				i := 0
				for e := range resultChan {
					c.So(string(e.GetPayload()), ShouldEqual, fmt.Sprintf("entry%d", 67 - i))
					i++
				}
			})

			c.Convey("returns length with lt and amount", FailureHalts, func(c C) {
				amount := 10
				resultChan := make(chan iface.IPFSLogEntry, 110)

				parsedCid, err := cid.Parse("bafyreigrbir6zv5cii7y7tw3z5fegsfujedcagd4ws45gzbtig52j4k2my")
				c.So(err, ShouldBeNil)

				err = log1.Iterator(&ipfslog.IteratorOptions{
					LT: []cid.Cid{parsedCid},
					Amount: &amount,
				}, resultChan)

				c.So(err, ShouldBeNil)
				c.So(len(resultChan), ShouldEqual, amount)
			})

			c.Convey("returns entries with lt and amount", FailureHalts, func(c C) {
				amount := 10
				resultChan := make(chan iface.IPFSLogEntry, 110)

				parsedCid, err := cid.Parse("bafyreigrbir6zv5cii7y7tw3z5fegsfujedcagd4ws45gzbtig52j4k2my")
				c.So(err, ShouldBeNil)

				err = log1.Iterator(&ipfslog.IteratorOptions{
					LT: []cid.Cid{parsedCid},
					Amount: &amount,
				}, resultChan)

				c.So(err, ShouldBeNil)

				i := 1
				for e := range resultChan {
					c.So(string(e.GetPayload()), ShouldEqual, fmt.Sprintf("entry%d", 67 - i))
					i++
				}
			})

			c.Convey("returns entries with gt and amount", FailureHalts, func(c C) {
				amount := 5
				resultChan := make(chan iface.IPFSLogEntry, 110)

				parsedCid, err := cid.Parse("bafyreigrbir6zv5cii7y7tw3z5fegsfujedcagd4ws45gzbtig52j4k2my")
				c.So(err, ShouldBeNil)

				err = log1.Iterator(&ipfslog.IteratorOptions{
					GT: &parsedCid,
					Amount: &amount,
				}, resultChan)

				c.So(err, ShouldBeNil)
				i := 0
				for e := range resultChan {
					c.So(string(e.GetPayload()), ShouldEqual, fmt.Sprintf("entry%d", 72 - i))
					i++
				}
			})

			c.Convey("returns length with gt and amount", FailureHalts, func(c C) {
				amount := 12
				resultChan := make(chan iface.IPFSLogEntry, 110)

				parsedCid, err := cid.Parse("bafyreigrbir6zv5cii7y7tw3z5fegsfujedcagd4ws45gzbtig52j4k2my")
				c.So(err, ShouldBeNil)

				err = log1.Iterator(&ipfslog.IteratorOptions{
					GT: &parsedCid,
					Amount: &amount,
				}, resultChan)

				c.So(err, ShouldBeNil)
				c.So(len(resultChan), ShouldEqual, amount)
			})

			c.Convey("returns entries with gte and amount", FailureHalts, func(c C) {
				amount := 5
				resultChan := make(chan iface.IPFSLogEntry, 110)

				parsedCid, err := cid.Parse("bafyreigrbir6zv5cii7y7tw3z5fegsfujedcagd4ws45gzbtig52j4k2my")
				c.So(err, ShouldBeNil)

				err = log1.Iterator(&ipfslog.IteratorOptions{
					GTE: &parsedCid,
					Amount: &amount,
				}, resultChan)

				c.So(err, ShouldBeNil)
				i := 1
				for e := range resultChan {
					c.So(string(e.GetPayload()), ShouldEqual, fmt.Sprintf("entry%d", 72 - i))
					i++
				}
			})

			c.Convey("returns length with gte and amount", FailureHalts, func(c C) {
				amount := 12
				resultChan := make(chan iface.IPFSLogEntry, 110)

				parsedCid, err := cid.Parse("bafyreigrbir6zv5cii7y7tw3z5fegsfujedcagd4ws45gzbtig52j4k2my")
				c.So(err, ShouldBeNil)

				err = log1.Iterator(&ipfslog.IteratorOptions{
					GTE: &parsedCid,
					Amount: &amount,
				}, resultChan)

				c.So(err, ShouldBeNil)
				c.So(len(resultChan), ShouldEqual, amount)
			})

			c.Convey("iterates with lt and gt", FailureHalts, func(c C) {
				resultChan := make(chan iface.IPFSLogEntry, 110)

				parsedCidLT, err := cid.Parse("bafyreiba2tj5pwleed6czd2p4n3xhwurnninp6bux3zopdstpughhy7ohy")
				c.So(err, ShouldBeNil)

				parsedCidGT, err := cid.Parse("bafyreiffkf2s7k56eaubd4qfuz6rnpahvqjg2c2hjm3bbms4zgarf3q7gq")
				c.So(err, ShouldBeNil)

				err = log1.Iterator(&ipfslog.IteratorOptions{
					LT: []cid.Cid{parsedCidLT},
					GT: &parsedCidGT,
				}, resultChan)

				c.So(err, ShouldBeNil)
				var hashes []string

				for e := range resultChan {
					hashes = append(hashes, string(e.GetPayload()))
				}

				c.So(hashes, ShouldNotContain, "bafyreiffkf2s7k56eaubd4qfuz6rnpahvqjg2c2hjm3bbms4zgarf3q7gq")
				c.So(hashes, ShouldNotContain, "bafyreiba2tj5pwleed6czd2p4n3xhwurnninp6bux3zopdstpughhy7ohy")
				c.So(len(hashes), ShouldEqual, 10)

			})

			c.Convey("iterates with lt and gte", FailureHalts, func(c C) {
				resultChan := make(chan iface.IPFSLogEntry, 110)

				parsedCidLT, err := cid.Parse("bafyreiba2tj5pwleed6czd2p4n3xhwurnninp6bux3zopdstpughhy7ohy")
				c.So(err, ShouldBeNil)

				parsedCidGT, err := cid.Parse("bafyreiffkf2s7k56eaubd4qfuz6rnpahvqjg2c2hjm3bbms4zgarf3q7gq")
				c.So(err, ShouldBeNil)

				err = log1.Iterator(&ipfslog.IteratorOptions{
					LT: []cid.Cid{parsedCidLT},
					GTE: &parsedCidGT,
				}, resultChan)

				c.So(err, ShouldBeNil)
				var hashes []string

				for e := range resultChan {
					hashes = append(hashes, e.GetHash().String())
				}

				c.So(hashes, ShouldContain, "bafyreiffkf2s7k56eaubd4qfuz6rnpahvqjg2c2hjm3bbms4zgarf3q7gq")
				c.So(hashes, ShouldNotContain, "bafyreiba2tj5pwleed6czd2p4n3xhwurnninp6bux3zopdstpughhy7ohy")
				c.So(len(hashes), ShouldEqual, 11)
			})

			c.Convey("iterates with lte and gt", FailureHalts, func(c C) {
				resultChan := make(chan iface.IPFSLogEntry, 110)

				parsedCidLT, err := cid.Parse("bafyreiba2tj5pwleed6czd2p4n3xhwurnninp6bux3zopdstpughhy7ohy")
				c.So(err, ShouldBeNil)

				parsedCidGT, err := cid.Parse("bafyreiffkf2s7k56eaubd4qfuz6rnpahvqjg2c2hjm3bbms4zgarf3q7gq")
				c.So(err, ShouldBeNil)

				err = log1.Iterator(&ipfslog.IteratorOptions{
					LTE: []cid.Cid{parsedCidLT},
					GT: &parsedCidGT,
				}, resultChan)

				c.So(err, ShouldBeNil)
				var hashes []string

				for e := range resultChan {
					hashes = append(hashes, e.GetHash().String())
				}

				c.So(hashes, ShouldNotContain, "bafyreiffkf2s7k56eaubd4qfuz6rnpahvqjg2c2hjm3bbms4zgarf3q7gq")
				c.So(hashes, ShouldContain, "bafyreiba2tj5pwleed6czd2p4n3xhwurnninp6bux3zopdstpughhy7ohy")
				c.So(len(hashes), ShouldEqual, 11)
			})

			c.Convey("iterates with lte and gte", FailureHalts, func(c C) {
				resultChan := make(chan iface.IPFSLogEntry, 110)

				parsedCidLT, err := cid.Parse("bafyreiba2tj5pwleed6czd2p4n3xhwurnninp6bux3zopdstpughhy7ohy")
				c.So(err, ShouldBeNil)

				parsedCidGT, err := cid.Parse("bafyreiffkf2s7k56eaubd4qfuz6rnpahvqjg2c2hjm3bbms4zgarf3q7gq")
				c.So(err, ShouldBeNil)

				err = log1.Iterator(&ipfslog.IteratorOptions{
					LTE: []cid.Cid{parsedCidLT},
					GTE: &parsedCidGT,
				}, resultChan)

				c.So(err, ShouldBeNil)
				var hashes []string

				for e := range resultChan {
					hashes = append(hashes, e.GetHash().String())
				}

				c.So(hashes, ShouldContain, "bafyreiffkf2s7k56eaubd4qfuz6rnpahvqjg2c2hjm3bbms4zgarf3q7gq")
				c.So(hashes, ShouldContain, "bafyreiba2tj5pwleed6czd2p4n3xhwurnninp6bux3zopdstpughhy7ohy")
				c.So(len(hashes), ShouldEqual, 12)
			})

			c.Convey("returns length with gt and default amount", FailureHalts, func(c C) {
				resultChan := make(chan iface.IPFSLogEntry, 110)

				parsedCid, err := cid.Parse("bafyreigrbir6zv5cii7y7tw3z5fegsfujedcagd4ws45gzbtig52j4k2my")
				c.So(err, ShouldBeNil)

				err = log1.Iterator(&ipfslog.IteratorOptions{
					GT: &parsedCid,
				}, resultChan)

				c.So(err, ShouldBeNil)
				c.So(len(resultChan), ShouldEqual, 33)
			})

			c.Convey("returns entries with gt and default amount", FailureHalts, func(c C) {
				resultChan := make(chan iface.IPFSLogEntry, 110)

				parsedCid, err := cid.Parse("bafyreigrbir6zv5cii7y7tw3z5fegsfujedcagd4ws45gzbtig52j4k2my")
				c.So(err, ShouldBeNil)

				err = log1.Iterator(&ipfslog.IteratorOptions{
					GT: &parsedCid,
				}, resultChan)

				c.So(err, ShouldBeNil)
				i := 0
				for e := range resultChan {
					c.So(string(e.GetPayload()), ShouldEqual, fmt.Sprintf("entry%d", 100 - i))
					i++
				}
			})

			c.Convey("returns length with gte and default amount", FailureHalts, func(c C) {
				resultChan := make(chan iface.IPFSLogEntry, 110)

				parsedCid, err := cid.Parse("bafyreigrbir6zv5cii7y7tw3z5fegsfujedcagd4ws45gzbtig52j4k2my")
				c.So(err, ShouldBeNil)

				err = log1.Iterator(&ipfslog.IteratorOptions{
					GTE: &parsedCid,
				}, resultChan)

				c.So(err, ShouldBeNil)
				c.So(len(resultChan), ShouldEqual, 34)
			})

			c.Convey("returns entries with gte and default amount", FailureHalts, func(c C) {
				resultChan := make(chan iface.IPFSLogEntry, 110)

				parsedCid, err := cid.Parse("bafyreigrbir6zv5cii7y7tw3z5fegsfujedcagd4ws45gzbtig52j4k2my")
				c.So(err, ShouldBeNil)

				err = log1.Iterator(&ipfslog.IteratorOptions{
					GTE: &parsedCid,
				}, resultChan)

				c.So(err, ShouldBeNil)
				i := 0
				for e := range resultChan {
					c.So(string(e.GetPayload()), ShouldEqual, fmt.Sprintf("entry%d", 100 - i))
					i++
				}
			})

			c.Convey("returns length with lt and default amount value", FailureHalts, func(c C) {
				resultChan := make(chan iface.IPFSLogEntry, 110)

				parsedCid, err := cid.Parse("bafyreigrbir6zv5cii7y7tw3z5fegsfujedcagd4ws45gzbtig52j4k2my")
				c.So(err, ShouldBeNil)

				err = log1.Iterator(&ipfslog.IteratorOptions{
					LT: []cid.Cid{parsedCid},
				}, resultChan)

				c.So(err, ShouldBeNil)
				c.So(len(resultChan), ShouldEqual, 67)
			})

			c.Convey("returns entries with lt and default amount value", FailureHalts, func(c C) {
				resultChan := make(chan iface.IPFSLogEntry, 110)

				parsedCid, err := cid.Parse("bafyreigrbir6zv5cii7y7tw3z5fegsfujedcagd4ws45gzbtig52j4k2my")
				c.So(err, ShouldBeNil)

				err = log1.Iterator(&ipfslog.IteratorOptions{
					LT: []cid.Cid{parsedCid},
				}, resultChan)

				c.So(err, ShouldBeNil)
				i := 0
				for e := range resultChan {
					c.So(string(e.GetPayload()), ShouldEqual, fmt.Sprintf("entry%d", 66 - i))
					i++
				}
			})

			c.Convey("returns length with lte and default amount value", FailureHalts, func(c C) {
				resultChan := make(chan iface.IPFSLogEntry, 110)

				parsedCid, err := cid.Parse("bafyreigrbir6zv5cii7y7tw3z5fegsfujedcagd4ws45gzbtig52j4k2my")
				c.So(err, ShouldBeNil)

				err = log1.Iterator(&ipfslog.IteratorOptions{
					LTE: []cid.Cid{parsedCid},
				}, resultChan)

				c.So(err, ShouldBeNil)
				c.So(len(resultChan), ShouldEqual, 68)

			})

			c.Convey("returns entries with lte and default amount value", FailureHalts, func(c C) {
				resultChan := make(chan iface.IPFSLogEntry, 110)

				parsedCid, err := cid.Parse("bafyreigrbir6zv5cii7y7tw3z5fegsfujedcagd4ws45gzbtig52j4k2my")
				c.So(err, ShouldBeNil)

				err = log1.Iterator(&ipfslog.IteratorOptions{
					LTE: []cid.Cid{parsedCid},
				}, resultChan)

				c.So(err, ShouldBeNil)
				i := 0
				for e := range resultChan {
					c.So(string(e.GetPayload()), ShouldEqual, fmt.Sprintf("entry%d", 67 - i))
					i++
				}

			})
		})

		c.Convey("Iteration over forked/joined logs", FailureHalts, func(c C) {
			identities := [4]*idp.Identity{identities[2], identities[1], identities[2], identities[0]}
			fixture, err := logcreator.CreateLogWithSixteenEntries(ctx, ipfs, identities)
			c.So(err, ShouldBeNil)

			c.Convey("returns the full length from all heads", FailureHalts, func(c C) {
				resultChan := make(chan iface.IPFSLogEntry, 110)

				var headsCids []cid.Cid
				for _, h := range fixture.Log.Heads().Slice() {
					headsCids = append(headsCids, h.GetHash())
				}

				err := fixture.Log.Iterator(&ipfslog.IteratorOptions{
					LTE: headsCids,
				}, resultChan)

				c.So(err, ShouldBeNil)

				c.So(len(resultChan), ShouldEqual, 16)
			})

			c.Convey("returns partial entries from all heads", FailureHalts, func(c C) {
				resultChan := make(chan iface.IPFSLogEntry, 110)

				var headsCids []cid.Cid
				for _, h := range fixture.Log.Heads().Slice() {
					headsCids = append(headsCids, h.GetHash())
				}

				err := fixture.Log.Iterator(&ipfslog.IteratorOptions{
					Amount: intPtr(6),
					LTE: headsCids,
				}, resultChan)

				var foundEntries []string
				expectedEntries := []string{"entryA10", "entryA9", "entryA8", "entryA7", "entryC0", "entryA6"}

				for e := range resultChan {
					foundEntries = append(foundEntries, string(e.GetPayload()))
				}


				c.So(err, ShouldBeNil)
				c.So(foundEntries, ShouldResemble, expectedEntries)
			})

			c.Convey("returns partial logs from single heads #1", FailureHalts, func(c C) {
				resultChan := make(chan iface.IPFSLogEntry, 110)

				headsCids := []cid.Cid{fixture.Log.Heads().At(0).GetHash()}

				err := fixture.Log.Iterator(&ipfslog.IteratorOptions{
					LTE: headsCids,
				}, resultChan)

				c.So(err, ShouldBeNil)
				c.So(len(resultChan), ShouldEqual, 10)
			})

			c.Convey("returns partial logs from single heads #2", FailureHalts, func(c C) {
				resultChan := make(chan iface.IPFSLogEntry, 110)

				headsCids := []cid.Cid{fixture.Log.Heads().At(1).GetHash()}

				err := fixture.Log.Iterator(&ipfslog.IteratorOptions{
					LTE: headsCids,
				}, resultChan)

				c.So(err, ShouldBeNil)
				c.So(len(resultChan), ShouldEqual, 11)
			})
		})
	})
}