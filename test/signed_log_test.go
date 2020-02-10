package test // import "berty.tech/go-ipfs-log/test"

import (
	"context"
	"fmt"
	"testing"
	"time"

	mocknet "github.com/libp2p/go-libp2p/p2p/net/mock"

	"berty.tech/go-ipfs-log/accesscontroller"

	dssync "github.com/ipfs/go-datastore/sync"

	ipfslog "berty.tech/go-ipfs-log"
	"berty.tech/go-ipfs-log/errmsg"
	idp "berty.tech/go-ipfs-log/identityprovider"
	ks "berty.tech/go-ipfs-log/keystore"

	. "github.com/smartystreets/goconvey/convey"
)

type DenyAll struct {
}

func (*DenyAll) CanAppend(accesscontroller.LogEntry, idp.Interface, accesscontroller.CanAppendAdditionalContext) error {
	return fmt.Errorf("denied")
}

type TestACL struct {
	refIdentity *idp.Identity
}

func (t *TestACL) CanAppend(e accesscontroller.LogEntry, p idp.Interface, _ accesscontroller.CanAppendAdditionalContext) error {
	if e.GetIdentity().ID == t.refIdentity.ID {
		return fmt.Errorf("denied")
	}

	return nil
}

func TestSignedLog(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*10)
	defer cancel()

	m := mocknet.New(ctx)
	ipfs, closeNode := NewMemoryServices(ctx, t, m)
	defer closeNode()

	datastore := dssync.MutexWrap(NewIdentityDataStore(t))
	keystore, err := ks.NewKeystore(datastore)
	if err != nil {
		t.Fatal(err)
	}

	var identities [4]*idp.Identity

	for i, char := range []rune{'A', 'B'} {

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

	Convey("Signed IPFSLog", t, FailureHalts, func(c C) {
		c.Convey("creates a signed log", FailureHalts, func(c C) {
			logID := "A"
			l, err := ipfslog.NewLog(ipfs, identities[0], &ipfslog.LogOptions{ID: logID})
			c.So(err, ShouldBeNil)
			c.So(l.ID, ShouldNotBeNil)
			c.So(l.ID, ShouldEqual, logID)
		})

		c.Convey("has the correct identity", FailureHalts, func(c C) {
			l, err := ipfslog.NewLog(ipfs, identities[0], &ipfslog.LogOptions{ID: "A"})
			c.So(err, ShouldBeNil)
			c.So(l.ID, ShouldNotBeNil)
			c.So(l.Identity.ID, ShouldEqual, "03e0480538c2a39951d054e17ff31fde487cb1031d0044a037b53ad2e028a3e77c")
			c.So(l.Identity.PublicKey, ShouldResemble, MustBytesFromHex(t, "048bef2231e64d5c7147bd4b8afb84abd4126ee8d8335e4b069ac0a65c7be711cea5c1b8d47bc20ebaecdca588600ddf2894675e78b2ef17cf49e7bbaf98080361"))
			c.So(l.Identity.Signatures.ID, ShouldResemble, MustBytesFromHex(t, "3045022100f5f6f10571d14347aaf34e526ce3419fd64d75ffa7aa73692cbb6aeb6fbc147102203a3e3fa41fa8fcbb9fc7c148af5b640e2f704b20b3a4e0b93fc3a6d44dffb41e"))
			c.So(l.Identity.Signatures.PublicKey, ShouldResemble, MustBytesFromHex(t, "3044022020982b8492be0c184dc29de0a3a3bd86a86ba997756b0bf41ddabd24b47c5acf02203745fda39d7df650a5a478e52bbe879f0cb45c074025a93471414a56077640a4"))
		})

		c.Convey("has the correct public key", FailureHalts, func(c C) {
			l, err := ipfslog.NewLog(ipfs, identities[0], &ipfslog.LogOptions{ID: "A"})
			c.So(err, ShouldBeNil)

			c.So(l.Identity.PublicKey, ShouldResemble, identities[0].PublicKey)
		})

		c.Convey("has the correct pkSignature", FailureHalts, func(c C) {
			l, err := ipfslog.NewLog(ipfs, identities[0], &ipfslog.LogOptions{ID: "A"})
			c.So(err, ShouldBeNil)

			c.So(l.Identity.Signatures.ID, ShouldResemble, identities[0].Signatures.ID)
		})

		c.Convey("has the correct signature", FailureHalts, func(c C) {
			l, err := ipfslog.NewLog(ipfs, identities[0], &ipfslog.LogOptions{ID: "A"})
			c.So(err, ShouldBeNil)

			c.So(l.Identity.Signatures.PublicKey, ShouldResemble, identities[0].Signatures.PublicKey)
		})

		c.Convey("entries contain an identity", FailureHalts, func(c C) {
			l, err := ipfslog.NewLog(ipfs, identities[0], &ipfslog.LogOptions{ID: "A"})
			c.So(err, ShouldBeNil)

			_, err = l.Append(ctx, []byte("one"), nil)
			c.So(err, ShouldBeNil)

			c.So(l.Values().At(0).GetSig(), ShouldNotBeNil)
			c.So(l.Values().At(0).GetIdentity().Filtered(), ShouldResemble, identities[0].Filtered())
		})

		c.Convey("doesn't sign entries when identity is not defined", FailureHalts, func(c C) {
			_, err := ipfslog.NewLog(ipfs, nil, nil)
			c.So(err, ShouldEqual, errmsg.IdentityNotDefined)
		})

		c.Convey("doesn't join logs with different IDs", FailureHalts, func(c C) {
			l1, err := ipfslog.NewLog(ipfs, identities[0], &ipfslog.LogOptions{ID: "A"})
			c.So(err, ShouldBeNil)

			l2, err := ipfslog.NewLog(ipfs, identities[0], &ipfslog.LogOptions{ID: "B"})
			c.So(err, ShouldBeNil)

			_, err = l1.Append(ctx, []byte("one"), nil)
			c.So(err, ShouldBeNil)

			_, err = l2.Append(ctx, []byte("two"), nil)
			c.So(err, ShouldBeNil)

			_, err = l2.Append(ctx, []byte("three"), nil)
			c.So(err, ShouldBeNil)

			_, err = l1.Join(l2, -1)
			c.So(err, ShouldBeNil)

			c.So(l1.ID, ShouldEqual, "A")
			c.So(l1.Values().Len(), ShouldEqual, 1)
			c.So(l1.Values().At(0).GetPayload(), ShouldResemble, []byte("one"))
		})

		c.Convey("throws an error if log is signed but trying to merge with an entry that doesn't have public signing key", FailureHalts, func(c C) {
			l1, err := ipfslog.NewLog(ipfs, identities[0], &ipfslog.LogOptions{ID: "A"})
			c.So(err, ShouldBeNil)

			l2, err := ipfslog.NewLog(ipfs, identities[1], &ipfslog.LogOptions{ID: "A"})
			c.So(err, ShouldBeNil)

			_, err = l1.Append(ctx, []byte("one"), nil)
			c.So(err, ShouldBeNil)

			_, err = l2.Append(ctx, []byte("two"), nil)
			c.So(err, ShouldBeNil)

			l2.Values().At(0).SetKey(nil)

			_, err = l1.Join(l2, -1)
			c.So(err, ShouldNotBeNil)
			c.So(err.Error(), ShouldContainSubstring, errmsg.KeyNotDefined.Error())
		})

		c.Convey("throws an error if log is signed but trying to merge an entry that doesn't have a signature", FailureHalts, func(c C) {
			l1, err := ipfslog.NewLog(ipfs, identities[0], &ipfslog.LogOptions{ID: "A"})
			c.So(err, ShouldBeNil)

			l2, err := ipfslog.NewLog(ipfs, identities[1], &ipfslog.LogOptions{ID: "A"})
			c.So(err, ShouldBeNil)

			_, err = l1.Append(ctx, []byte("one"), nil)
			c.So(err, ShouldBeNil)

			_, err = l2.Append(ctx, []byte("two"), nil)
			c.So(err, ShouldBeNil)

			l2.Values().At(0).SetSig(nil)

			_, err = l1.Join(l2, -1)
			c.So(err, ShouldNotBeNil)
			c.So(err.Error(), ShouldContainSubstring, errmsg.SigNotDefined.Error())
		})

		c.Convey("throws an error if log is signed but the signature doesn't verify", FailureHalts, func(c C) {
			l1, err := ipfslog.NewLog(ipfs, identities[0], &ipfslog.LogOptions{ID: "A"})
			c.So(err, ShouldBeNil)

			l2, err := ipfslog.NewLog(ipfs, identities[1], &ipfslog.LogOptions{ID: "A"})
			c.So(err, ShouldBeNil)

			_, err = l1.Append(ctx, []byte("one"), nil)
			c.So(err, ShouldBeNil)

			_, err = l2.Append(ctx, []byte("two"), nil)
			c.So(err, ShouldBeNil)

			l2.Values().At(0).SetSig(l1.Values().At(0).GetSig())

			_, err = l1.Join(l2, -1)
			c.So(err, ShouldNotBeNil)
			c.So(err.Error(), ShouldContainSubstring, errmsg.SigNotVerified.Error())

			c.So(l1.Values().Len(), ShouldEqual, 1)
			c.So(l1.Values().At(0).GetPayload(), ShouldResemble, []byte("one"))
		})

		c.Convey("throws an error if entry doesn't have append access", FailureHalts, func(c C) {
			l1, err := ipfslog.NewLog(ipfs, identities[0], &ipfslog.LogOptions{ID: "A"})
			c.So(err, ShouldBeNil)

			l2, err := ipfslog.NewLog(ipfs, identities[1], &ipfslog.LogOptions{ID: "A", AccessController: &DenyAll{}})
			c.So(err, ShouldBeNil)

			_, err = l1.Append(ctx, []byte("one"), nil)
			c.So(err, ShouldBeNil)

			_, err = l2.Append(ctx, []byte("two"), nil)
			c.So(err, ShouldNotBeNil)
			c.So(err.Error(), ShouldContainSubstring, errmsg.LogAppendDenied.Error())
		})

		c.Convey("throws an error upon join if entry doesn't have append access", FailureHalts, func(c C) {
			l1, err := ipfslog.NewLog(ipfs, identities[0], &ipfslog.LogOptions{ID: "A", AccessController: &TestACL{refIdentity: identities[1]}})
			c.So(err, ShouldBeNil)

			l2, err := ipfslog.NewLog(ipfs, identities[1], &ipfslog.LogOptions{ID: "A"})
			c.So(err, ShouldBeNil)

			_, err = l1.Append(ctx, []byte("one"), nil)
			c.So(err, ShouldBeNil)

			_, err = l2.Append(ctx, []byte("two"), nil)
			c.So(err, ShouldBeNil)

			_, err = l1.Join(l2, -1)
			c.So(err, ShouldNotBeNil)
			c.So(err.Error(), ShouldContainSubstring, errmsg.LogJoinFailed.Error())
		})
	})
}
