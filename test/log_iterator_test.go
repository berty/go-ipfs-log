package test

import (
	"context"
	"fmt"
	"testing"

	"github.com/ipfs/go-cid"
	mocknet "github.com/libp2p/go-libp2p/p2p/net/mock"
	. "github.com/smartystreets/goconvey/convey"

	"berty.tech/go-ipfs-log/iface"

	ipfslog "berty.tech/go-ipfs-log"

	dssync "github.com/ipfs/go-datastore/sync"

	idp "berty.tech/go-ipfs-log/identityprovider"
	ks "berty.tech/go-ipfs-log/keystore"
)

func TestLogIterator(t *testing.T) {
	ctx := context.Background()

	Convey("IPFSLog - Iterator", t, FailureContinues, func(c C) {
		datastore := dssync.MutexWrap(NewIdentityDataStore(t))
		keystore, err := ks.NewKeystore(datastore)
		if err != nil {
			t.Fatal(err)
		}

		identities := make([]*idp.Identity, 4)

		for i, char := range []rune{'A', 'B', 'C'} {
			identity, err := idp.CreateIdentity(&idp.CreateIdentityOptions{
				Keystore: keystore,
				ID:       fmt.Sprintf("user%c", char),
				Type:     "orbitdb",
			})

			if err != nil {
				t.Fatal(err)
			}

			identities[i] = identity
		}

		m := mocknet.New(ctx)
		ipfs, closeNode := NewMemoryServices(ctx, t, m)
		defer closeNode()

		log1, err := ipfslog.NewLog(ipfs, identities[0], &ipfslog.LogOptions{ID: "X"})
		c.So(err, ShouldBeNil)

		for i := 0; i <= 100; i++ {
			_, err := log1.Append(ctx, []byte(fmt.Sprintf("entry%d", i)), nil)
			c.So(err, ShouldBeNil)
		}

		// Basic iterator functionality
		// returns length with lte and amount

		amount := 10
		resultChan := make(chan iface.IPFSLogEntry, 110)

		idLTE := MustCID(t, "zdpuAuNuQ4YBeXY5YStfrsJx6ykz4yBV2XnNcBR4uGmiojQde")

		err = log1.Iterator(&ipfslog.IteratorOptions{
			LTE:    []cid.Cid{idLTE},
			Amount: &amount,
		}, resultChan)

		c.So(err, ShouldBeNil)
		c.So(len(resultChan), ShouldEqual, amount)

		// returns entries with lte and amount

		amount = 10
		resultChan = make(chan iface.IPFSLogEntry, 110)

		idLTE = MustCID(t, "zdpuAuNuQ4YBeXY5YStfrsJx6ykz4yBV2XnNcBR4uGmiojQde")

		err = log1.Iterator(&ipfslog.IteratorOptions{
			LTE:    []cid.Cid{idLTE},
			Amount: &amount,
		}, resultChan)

		c.So(err, ShouldBeNil)

		i := 0
		for e := range resultChan {
			c.So(string(e.GetPayload()), ShouldEqual, fmt.Sprintf("entry%d", 67-i))
			i++
		}

		// returns length with lt and amount

		amount = 10
		resultChan = make(chan iface.IPFSLogEntry, 110)

		idLT := MustCID(t, "zdpuAuNuQ4YBeXY5YStfrsJx6ykz4yBV2XnNcBR4uGmiojQde")

		err = log1.Iterator(&ipfslog.IteratorOptions{
			LT:     []cid.Cid{idLT},
			Amount: &amount,
		}, resultChan)

		c.So(err, ShouldBeNil)
		c.So(len(resultChan), ShouldEqual, amount)

		// returns entries with lt and amount

		amount = 10
		resultChan = make(chan iface.IPFSLogEntry, 110)

		idLT = MustCID(t, "zdpuAuNuQ4YBeXY5YStfrsJx6ykz4yBV2XnNcBR4uGmiojQde")

		err = log1.Iterator(&ipfslog.IteratorOptions{
			LT:     []cid.Cid{idLT},
			Amount: &amount,
		}, resultChan)

		c.So(err, ShouldBeNil)

		i = 1
		for e := range resultChan {
			c.So(string(e.GetPayload()), ShouldEqual, fmt.Sprintf("entry%d", 67-i))
			i++
		}

		// returns entries with gt and amount

		amount = 5
		resultChan = make(chan iface.IPFSLogEntry, 110)

		idGT := MustCID(t, "zdpuAuNuQ4YBeXY5YStfrsJx6ykz4yBV2XnNcBR4uGmiojQde")

		err = log1.Iterator(&ipfslog.IteratorOptions{
			GT:     idGT,
			Amount: &amount,
		}, resultChan)

		c.So(err, ShouldBeNil)
		i = 0
		for e := range resultChan {
			c.So(string(e.GetPayload()), ShouldEqual, fmt.Sprintf("entry%d", 72-i))
			i++
		}

		// returns length with gt and amount

		amount = 12
		resultChan = make(chan iface.IPFSLogEntry, 110)
		idGT = MustCID(t, "zdpuAuNuQ4YBeXY5YStfrsJx6ykz4yBV2XnNcBR4uGmiojQde")

		err = log1.Iterator(&ipfslog.IteratorOptions{
			GT:     idGT,
			Amount: &amount,
		}, resultChan)

		c.So(err, ShouldBeNil)
		c.So(len(resultChan), ShouldEqual, amount)

		// returns entries with gte and amount

		amount = 5
		resultChan = make(chan iface.IPFSLogEntry, 110)

		idGTE := MustCID(t, "zdpuAuNuQ4YBeXY5YStfrsJx6ykz4yBV2XnNcBR4uGmiojQde")

		err = log1.Iterator(&ipfslog.IteratorOptions{
			GTE:    idGTE,
			Amount: &amount,
		}, resultChan)

		c.So(err, ShouldBeNil)
		i = 1
		for e := range resultChan {
			c.So(string(e.GetPayload()), ShouldEqual, fmt.Sprintf("entry%d", 72-i))
			i++
		}

		// returns length with gte and amount

		amount = 12
		resultChan = make(chan iface.IPFSLogEntry, 110)

		idGTE = MustCID(t, "zdpuAuNuQ4YBeXY5YStfrsJx6ykz4yBV2XnNcBR4uGmiojQde")

		err = log1.Iterator(&ipfslog.IteratorOptions{
			GTE:    idGTE,
			Amount: &amount,
		}, resultChan)

		c.So(err, ShouldBeNil)
		c.So(len(resultChan), ShouldEqual, amount)

		// iterates with lt and gt

		resultChan = make(chan iface.IPFSLogEntry, 110)
		idGT = MustCID(t, "zdpuAymZUrYbHgwfYK76xXYhzxNqwaXRWWrn5kmRsZJFdqBEz")
		idLT = MustCID(t, "zdpuAoDcWRiChLXnGskymcGrM1VdAjsaFrsXvNZmcDattA7AF")

		err = log1.Iterator(&ipfslog.IteratorOptions{
			GT: idGT,
			LT: []cid.Cid{idLT},
		}, resultChan)

		c.So(err, ShouldBeNil)
		hashes := []string(nil)

		for e := range resultChan {
			hashes = append(hashes, string(e.GetPayload()))
		}

		c.So(hashes, ShouldNotContain, idLT.String())
		c.So(hashes, ShouldNotContain, idGT.String())
		c.So(len(hashes), ShouldEqual, 10)

		// iterates with lt and gte

		resultChan = make(chan iface.IPFSLogEntry, 110)
		idGTE = MustCID(t, "zdpuAt7YtNE1i9APJitGyKomcmxjc2BDHa57wkrjq4onqBNaR")
		idLT = MustCID(t, "zdpuAr8N4vzqcB5sh5JLcr6Eszo4HnYefBWDbBBwwrTPo6kU6")

		err = log1.Iterator(&ipfslog.IteratorOptions{
			LT:  []cid.Cid{idLT},
			GTE: idGTE,
		}, resultChan)

		c.So(err, ShouldBeNil)
		hashes = []string(nil)

		for e := range resultChan {
			hashes = append(hashes, e.GetHash().String())
		}

		c.So(hashes, ShouldContain, idGTE.String())
		c.So(hashes, ShouldNotContain, idLT.String())
		c.So(len(hashes), ShouldEqual, 25)

		// iterates with lte and gt

		resultChan = make(chan iface.IPFSLogEntry, 110)
		idGT = MustCID(t, "zdpuAqUrGrPa4AaZAQbCH4yxQfEjB32rdFY743XCgyGW8iAuU")
		idLTE = MustCID(t, "zdpuAwkagwE9D2jUtLnDiCPqBGh9xhpnaX8iEDQ3K7HRmjggi")

		err = log1.Iterator(&ipfslog.IteratorOptions{
			LTE: []cid.Cid{idLTE},
			GT:  idGT,
		}, resultChan)

		c.So(err, ShouldBeNil)
		hashes = []string(nil)

		for e := range resultChan {
			hashes = append(hashes, e.GetHash().String())
		}

		c.So(hashes, ShouldNotContain, idGT.String())
		c.So(hashes, ShouldContain, idLTE.String())
		c.So(len(hashes), ShouldEqual, 4)

		// iterates with lte and gte

		resultChan = make(chan iface.IPFSLogEntry, 110)
		idGTE = MustCID(t, "zdpuAzG5AD1GdeNffSskTErjjPbAb95QiNyoaQSrbB62eqYSD")
		idLTE = MustCID(t, "zdpuAuujURnUUxVw338Xwh47zGEFjjbaZXXARHPik6KYUcUVk")

		err = log1.Iterator(&ipfslog.IteratorOptions{
			LTE: []cid.Cid{idLTE},
			GTE: idGTE,
		}, resultChan)

		c.So(err, ShouldBeNil)
		hashes = []string(nil)

		for e := range resultChan {
			hashes = append(hashes, e.GetHash().String())
		}

		c.So(hashes, ShouldContain, idGTE.String())
		c.So(hashes, ShouldContain, idLTE.String())
		c.So(len(hashes), ShouldEqual, 10)

		// returns length with gt and default amount

		resultChan = make(chan iface.IPFSLogEntry, 110)
		idGT = MustCID(t, "zdpuAuNuQ4YBeXY5YStfrsJx6ykz4yBV2XnNcBR4uGmiojQde")

		err = log1.Iterator(&ipfslog.IteratorOptions{
			GT: idGT,
		}, resultChan)

		c.So(err, ShouldBeNil)
		c.So(len(resultChan), ShouldEqual, 33)

		// returns entries with gt and default amount

		resultChan = make(chan iface.IPFSLogEntry, 110)
		idGT = MustCID(t, "zdpuAuNuQ4YBeXY5YStfrsJx6ykz4yBV2XnNcBR4uGmiojQde")
		c.So(err, ShouldBeNil)

		err = log1.Iterator(&ipfslog.IteratorOptions{
			GT: idGT,
		}, resultChan)

		c.So(err, ShouldBeNil)
		i = 0
		for e := range resultChan {
			c.So(string(e.GetPayload()), ShouldEqual, fmt.Sprintf("entry%d", 100-i))
			i++
		}

		// returns length with gte and default amount

		resultChan = make(chan iface.IPFSLogEntry, 110)
		idGTE = MustCID(t, "zdpuAuNuQ4YBeXY5YStfrsJx6ykz4yBV2XnNcBR4uGmiojQde")

		err = log1.Iterator(&ipfslog.IteratorOptions{
			GTE: idGTE,
		}, resultChan)

		c.So(err, ShouldBeNil)
		c.So(len(resultChan), ShouldEqual, 34)

		// returns entries with gte and default amount

		resultChan = make(chan iface.IPFSLogEntry, 110)
		idGTE = MustCID(t, "zdpuAuNuQ4YBeXY5YStfrsJx6ykz4yBV2XnNcBR4uGmiojQde")

		err = log1.Iterator(&ipfslog.IteratorOptions{
			GTE: idGTE,
		}, resultChan)

		c.So(err, ShouldBeNil)
		i = 0
		for e := range resultChan {
			c.So(string(e.GetPayload()), ShouldEqual, fmt.Sprintf("entry%d", 100-i))
			i++
		}

		// returns length with lt and default amount value

		resultChan = make(chan iface.IPFSLogEntry, 110)
		idLT = MustCID(t, "zdpuAuNuQ4YBeXY5YStfrsJx6ykz4yBV2XnNcBR4uGmiojQde")

		err = log1.Iterator(&ipfslog.IteratorOptions{
			LT: []cid.Cid{idLT},
		}, resultChan)

		c.So(err, ShouldBeNil)
		c.So(len(resultChan), ShouldEqual, 67)

		// returns entries with lt and default amount value

		resultChan = make(chan iface.IPFSLogEntry, 110)
		idLT = MustCID(t, "zdpuAuNuQ4YBeXY5YStfrsJx6ykz4yBV2XnNcBR4uGmiojQde")

		err = log1.Iterator(&ipfslog.IteratorOptions{
			LT: []cid.Cid{idLT},
		}, resultChan)

		c.So(err, ShouldBeNil)
		i = 0
		for e := range resultChan {
			c.So(string(e.GetPayload()), ShouldEqual, fmt.Sprintf("entry%d", 66-i))
			i++
		}

		// returns length with lte and default amount value

		resultChan = make(chan iface.IPFSLogEntry, 110)
		idLT = MustCID(t, "zdpuAuNuQ4YBeXY5YStfrsJx6ykz4yBV2XnNcBR4uGmiojQde")

		err = log1.Iterator(&ipfslog.IteratorOptions{
			LTE: []cid.Cid{idLT},
		}, resultChan)

		c.So(err, ShouldBeNil)
		c.So(len(resultChan), ShouldEqual, 68)

		// returns entries with lte and default amount value

		resultChan = make(chan iface.IPFSLogEntry, 110)
		idLTE = MustCID(t, "zdpuAuNuQ4YBeXY5YStfrsJx6ykz4yBV2XnNcBR4uGmiojQde")

		err = log1.Iterator(&ipfslog.IteratorOptions{
			LTE: []cid.Cid{idLTE},
		}, resultChan)

		c.So(err, ShouldBeNil)
		i = 0
		for e := range resultChan {
			c.So(string(e.GetPayload()), ShouldEqual, fmt.Sprintf("entry%d", 67-i))
			i++
		}

		// Iteration over forked/joined logs

		identities = []*idp.Identity{identities[2], identities[1], identities[2], identities[0]}
		fixture, err := CreateLogWithSixteenEntries(ctx, ipfs, identities)
		c.So(err, ShouldBeNil)

		// returns the full length from all heads

		resultChan = make(chan iface.IPFSLogEntry, 110)
		headsCids := []cid.Cid(nil)
		for _, h := range fixture.Log.Heads().Slice() {
			headsCids = append(headsCids, h.GetHash())
		}

		err = fixture.Log.Iterator(&ipfslog.IteratorOptions{
			LTE: headsCids,
		}, resultChan)

		c.So(err, ShouldBeNil)

		c.So(len(resultChan), ShouldEqual, 16)

		// returns partial entries from all heads

		resultChan = make(chan iface.IPFSLogEntry, 110)
		headsCids = []cid.Cid(nil)
		for _, h := range fixture.Log.Heads().Slice() {
			headsCids = append(headsCids, h.GetHash())
		}

		err = fixture.Log.Iterator(&ipfslog.IteratorOptions{
			Amount: intPtr(6),
			LTE:    headsCids,
		}, resultChan)

		var foundEntries []string
		expectedEntries := []string{"entryA10", "entryA9", "entryA8", "entryA7", "entryC0", "entryA6"}

		for e := range resultChan {
			foundEntries = append(foundEntries, string(e.GetPayload()))
		}

		c.So(err, ShouldBeNil)
		c.So(foundEntries, ShouldResemble, expectedEntries)

		// returns partial logs from single heads #1

		resultChan = make(chan iface.IPFSLogEntry, 110)
		headsCids = []cid.Cid{fixture.Log.Heads().At(0).GetHash()}

		err = fixture.Log.Iterator(&ipfslog.IteratorOptions{
			LTE: headsCids,
		}, resultChan)

		c.So(err, ShouldBeNil)
		c.So(len(resultChan), ShouldEqual, 10)

		// returns partial logs from single heads #2

		resultChan = make(chan iface.IPFSLogEntry, 110)
		headsCids = []cid.Cid{fixture.Log.Heads().At(1).GetHash()}

		err = fixture.Log.Iterator(&ipfslog.IteratorOptions{
			LTE: headsCids,
		}, resultChan)

		c.So(err, ShouldBeNil)
		c.So(len(resultChan), ShouldEqual, 11)
	})
}
