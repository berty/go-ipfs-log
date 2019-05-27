package test

import (
	"context"
	"fmt"
	"testing"
	"time"

	idp "github.com/berty/go-ipfs-log/identityprovider"
	"github.com/berty/go-ipfs-log/io"
	ks "github.com/berty/go-ipfs-log/keystore"
	"github.com/berty/go-ipfs-log/log"
	ds "github.com/ipfs/go-datastore"
	dssync "github.com/ipfs/go-datastore/sync"

	. "github.com/smartystreets/goconvey/convey"
)

func TestLogHeadsTails(t *testing.T) {
	_, cancel := context.WithTimeout(context.Background(), time.Second*10)
	defer cancel()

	ipfs := io.NewMemoryServices()

	datastore := dssync.MutexWrap(ds.NewMapDatastore())
	keystore, err := ks.NewKeystore(datastore)
	if err != nil {
		panic(err)
	}

	var identities []*idp.Identity

	for i := 0; i < 4; i++ {
		char := 'A' + i

		identity, err := idp.CreateIdentity(&idp.CreateIdentityOptions{
			Keystore: keystore,
			ID: fmt.Sprintf("user%c", char),
			Type: "orbitdb",
		})

		if err != nil {
			panic(err)
		}

		identities = append(identities, identity)
	}

	Convey("Log - Heads and Tails", t, FailureContinues, func(c C) {
		c.Convey("heads", FailureContinues, func(c C) {
			c.Convey("finds one head after one entry", FailureContinues, func(c C) {
				log1, err := log.NewLog(ipfs, identities[0], &log.NewLogOptions{ID: "A"})
				c.So(err, ShouldBeNil)
				_, err = log1.Append([]byte("helloA1"), 1)
				c.So(err, ShouldBeNil)

				c.So(len(log.FindHeads(log1.Entries)), ShouldEqual, 1)
			})

			c.Convey("finds one head after two entry", FailureContinues, func(c C) {
				log1, err := log.NewLog(ipfs, identities[0], &log.NewLogOptions{ID: "A"})
				c.So(err, ShouldBeNil)
				_, err = log1.Append([]byte("helloA1"), 1)
				c.So(err, ShouldBeNil)
				_, err = log1.Append([]byte("helloA2"), 1)
				c.So(err, ShouldBeNil)

				c.So(len(log.FindHeads(log1.Entries)), ShouldEqual, 1)
			})

			c.Convey("finds head after a join and append", FailureContinues, func(c C) {
				log1, err := log.NewLog(ipfs, identities[0], &log.NewLogOptions{ID: "A"})
				c.So(err, ShouldBeNil)
				log2, err := log.NewLog(ipfs, identities[0], &log.NewLogOptions{ID: "A"})
				c.So(err, ShouldBeNil)

				_, err = log1.Append([]byte("helloA1"), 1)
				c.So(err, ShouldBeNil)
				_, err = log1.Append([]byte("helloA2"), 1)
				c.So(err, ShouldBeNil)
				_, err = log2.Append([]byte("helloB1"), 1)
				c.So(err, ShouldBeNil)

				_, err = log2.Join(log1, -1)
				c.So(err, ShouldBeNil)
				_, err = log2.Append([]byte("helloB2"), 1)
				c.So(err, ShouldBeNil)

				lastEntry := getLastEntry(log2.Values())

				c.So(len(log.FindHeads(log2.Entries)), ShouldEqual, 1)
				c.So(log.FindHeads(log2.Entries)[0].Hash.String(), ShouldEqual, lastEntry.Hash.String())
			})

			c.Convey("finds two heads after a join", FailureContinues, func(c C) {
				log1, err := log.NewLog(ipfs, identities[0], &log.NewLogOptions{ID: "A"})
				c.So(err, ShouldBeNil)
				log2, err := log.NewLog(ipfs, identities[0], &log.NewLogOptions{ID: "A"})
				c.So(err, ShouldBeNil)

				_, err = log1.Append([]byte("helloA1"), 1)
				c.So(err, ShouldBeNil)
				_, err = log1.Append([]byte("helloA2"), 1)
				c.So(err, ShouldBeNil)
				lastEntry1 := getLastEntry(log1.Values())

				_, err = log2.Append([]byte("helloB1"), 1)
				c.So(err, ShouldBeNil)
				_, err = log2.Append([]byte("helloB2"), 1)
				c.So(err, ShouldBeNil)
				lastEntry2 := getLastEntry(log2.Values())

				_, err = log1.Join(log2, -1)
				c.So(err, ShouldBeNil)

				c.So(len(log.FindHeads(log1.Entries)), ShouldEqual, 2)
				c.So(log.FindHeads(log1.Entries)[0].Hash.String(), ShouldEqual, lastEntry1.Hash.String())
				c.So(log.FindHeads(log1.Entries)[1].Hash.String(), ShouldEqual, lastEntry2.Hash.String())
			})

			c.Convey("finds two heads after two joins", FailureContinues, func(c C) {
				log1, err := log.NewLog(ipfs, identities[0], &log.NewLogOptions{ID: "A"})
				c.So(err, ShouldBeNil)
				log2, err := log.NewLog(ipfs, identities[0], &log.NewLogOptions{ID: "A"})
				c.So(err, ShouldBeNil)

				_, err = log1.Append([]byte("helloA1"), 1)
				c.So(err, ShouldBeNil)
				_, err = log1.Append([]byte("helloA2"), 1)
				c.So(err, ShouldBeNil)

				_, err = log2.Append([]byte("helloB1"), 1)
				c.So(err, ShouldBeNil)
				_, err = log2.Append([]byte("helloB2"), 1)
				c.So(err, ShouldBeNil)

				_, err = log1.Join(log2, -1)
				c.So(err, ShouldBeNil)

				_, err = log2.Append([]byte("helloB3"), 1)
				c.So(err, ShouldBeNil)

				_, err = log1.Append([]byte("helloA3"), 1)
				c.So(err, ShouldBeNil)
				_, err = log1.Append([]byte("helloA4"), 1)
				c.So(err, ShouldBeNil)

				lastEntry1 := getLastEntry(log1.Values())
				lastEntry2 := getLastEntry(log2.Values())

				_, err = log1.Join(log2, -1)
				c.So(err, ShouldBeNil)

				c.So(len(log.FindHeads(log1.Entries)), ShouldEqual, 2)
				c.So(log.FindHeads(log1.Entries)[0].Hash.String(), ShouldEqual, lastEntry1.Hash.String())
				c.So(log.FindHeads(log1.Entries)[1].Hash.String(), ShouldEqual, lastEntry2.Hash.String())
			})

			c.Convey("finds two heads after three joins", FailureContinues, func(c C) {
				log1, err := log.NewLog(ipfs, identities[0], &log.NewLogOptions{ID: "A"})
				c.So(err, ShouldBeNil)
				log2, err := log.NewLog(ipfs, identities[0], &log.NewLogOptions{ID: "A"})
				c.So(err, ShouldBeNil)
				log3, err := log.NewLog(ipfs, identities[0], &log.NewLogOptions{ID: "A"})
				c.So(err, ShouldBeNil)

				_, err = log1.Append([]byte("helloA1"), 1)
				c.So(err, ShouldBeNil)
				_, err = log1.Append([]byte("helloA2"), 1)
				c.So(err, ShouldBeNil)
				_, err = log2.Append([]byte("helloB1"), 1)
				c.So(err, ShouldBeNil)
				_, err = log2.Append([]byte("helloB2"), 1)
				c.So(err, ShouldBeNil)
				_, err = log1.Join(log2, -1)
				c.So(err, ShouldBeNil)
				_, err = log1.Append([]byte("helloA3"), 1)
				c.So(err, ShouldBeNil)
				_, err = log1.Append([]byte("helloA4"), 1)
				c.So(err, ShouldBeNil)
				lastEntry1 := getLastEntry(log1.Values())
				_, err = log3.Append([]byte("helloC1"), 1)
				c.So(err, ShouldBeNil)
				_, err = log3.Append([]byte("helloC2"), 1)
				c.So(err, ShouldBeNil)
				_, err = log2.Join(log3, -1)
				c.So(err, ShouldBeNil)
				_, err = log2.Append([]byte("helloB3"), 1)
				c.So(err, ShouldBeNil)
				lastEntry2 := getLastEntry(log2.Values())
				_, err = log1.Join(log2, -1)
				c.So(err, ShouldBeNil)

				c.So(len(log.FindHeads(log1.Entries)), ShouldEqual, 2)
				c.So(log.FindHeads(log1.Entries)[0].Hash.String(), ShouldEqual, lastEntry1.Hash.String())
				c.So(log.FindHeads(log1.Entries)[1].Hash.String(), ShouldEqual, lastEntry2.Hash.String())
			})

			c.Convey("finds three heads after three joins", FailureContinues, func(c C) {
				log1, err := log.NewLog(ipfs, identities[0], &log.NewLogOptions{ID: "A"})
				c.So(err, ShouldBeNil)
				log2, err := log.NewLog(ipfs, identities[0], &log.NewLogOptions{ID: "A"})
				c.So(err, ShouldBeNil)
				log3, err := log.NewLog(ipfs, identities[0], &log.NewLogOptions{ID: "A"})
				c.So(err, ShouldBeNil)

				_, err = log1.Append([]byte("helloA1"), 1)
				c.So(err, ShouldBeNil)
				_, err = log1.Append([]byte("helloA2"), 1)
				c.So(err, ShouldBeNil)
				_, err = log2.Append([]byte("helloB1"), 1)
				c.So(err, ShouldBeNil)
				_, err = log2.Append([]byte("helloB2"), 1)
				c.So(err, ShouldBeNil)
				_, err = log1.Join(log2, -1)
				c.So(err, ShouldBeNil)
				_, err = log1.Append([]byte("helloA3"), 1)
				c.So(err, ShouldBeNil)
				_, err = log1.Append([]byte("helloA4"), 1)
				c.So(err, ShouldBeNil)
				lastEntry1 := getLastEntry(log1.Values())
				_, err = log3.Append([]byte("helloC1"), 1)
				c.So(err, ShouldBeNil)
				_, err = log2.Append([]byte("helloB3"), 1)
				c.So(err, ShouldBeNil)
				_, err = log3.Append([]byte("helloC2"), 1)
				c.So(err, ShouldBeNil)
				lastEntry2 := getLastEntry(log2.Values())
				lastEntry3 := getLastEntry(log3.Values())
				_, err = log1.Join(log2, -1)
				c.So(err, ShouldBeNil)
				_, err = log1.Join(log3, -1)
				c.So(err, ShouldBeNil)

				c.So(len(log.FindHeads(log1.Entries)), ShouldEqual, 3)
				c.So(log.FindHeads(log1.Entries)[0].Hash.String(), ShouldEqual, lastEntry1.Hash.String())
				c.So(log.FindHeads(log1.Entries)[1].Hash.String(), ShouldEqual, lastEntry2.Hash.String())
				c.So(log.FindHeads(log1.Entries)[2].Hash.String(), ShouldEqual, lastEntry3.Hash.String())
			})
		})

		c.Convey("tails", FailureContinues, func(c C) {
			// TODO: implements findTails(orderedmap)
			// c.Convey("returns a tail", FailureContinues, func(c C) {
			// 	log1, err := log.NewLog(ipfs, identities[0], &log.NewLogOptions{ID: "A"})
			// 	c.So(err, ShouldBeNil)
			// 	_, err = log1.Append([]byte("helloA1"), 1)
			// 	c.So(err, ShouldBeNil)

			// 	c.So(len(log.FindTails(log1.Entries)), ShouldEqual, 1)
			// })

			// c.Convey("returns tail entries", FailureContinues, func(c C) {
			// 	log1, err := log.NewLog(ipfs, identities[0], &log.NewLogOptions{ID: "A"})
			// 	c.So(err, ShouldBeNil)
			// 	log2, err := log.NewLog(ipfs, identities[0], &log.NewLogOptions{ID: "A"})
			// 	c.So(err, ShouldBeNil)
			// 	_, err = log1.Append([]byte("helloA1"), 1)
			// 	c.So(err, ShouldBeNil)
			// 	_, err = log1.Append([]byte("helloA1"), 1)
			// 	c.So(err, ShouldBeNil)

			// 	c.So(len(log.FindTails(log1.Entries)), ShouldEqual, 1)
			// })
		})
	})
}
