package test

import (
	"context"
	"fmt"
	"testing"

	ipfslog "berty.tech/go-ipfs-log"
	"berty.tech/go-ipfs-log/accesscontroller"
	"berty.tech/go-ipfs-log/errmsg"
	idp "berty.tech/go-ipfs-log/identityprovider"
	ks "berty.tech/go-ipfs-log/keystore"
	dssync "github.com/ipfs/go-datastore/sync"
	mocknet "github.com/libp2p/go-libp2p/p2p/net/mock"
	"github.com/stretchr/testify/require"
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
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	m := mocknet.New()
	defer m.Close()

	p, err := m.GenPeer()
	require.NoError(t, err)

	dag := setupDAGService(t, p)

	datastore := dssync.MutexWrap(NewIdentityDataStore(t))
	keystore, err := ks.NewKeystore(datastore)
	require.NoError(t, err)

	var identities [4]*idp.Identity

	for i, char := range []rune{'A', 'B'} {

		identity, err := idp.CreateIdentity(ctx, &idp.CreateIdentityOptions{
			Keystore: keystore,
			ID:       fmt.Sprintf("user%c", char),
			Type:     "orbitdb",
		})
		require.NoError(t, err)

		identities[i] = identity
	}

	t.Run("creates a signed log", func(t *testing.T) {
		logID := "A"
		l, err := ipfslog.NewLog(dag, identities[0], &ipfslog.LogOptions{ID: logID})
		require.NoError(t, err)
		require.NotNil(t, l.ID)
		require.Equal(t, l.ID, logID)
	})

	t.Run("has the correct identity", func(t *testing.T) {
		l, err := ipfslog.NewLog(dag, identities[0], &ipfslog.LogOptions{ID: "A"})
		require.NoError(t, err)
		require.NotNil(t, l.ID)
		require.Equal(t, l.Identity.ID, "03e0480538c2a39951d054e17ff31fde487cb1031d0044a037b53ad2e028a3e77c")
		require.Equal(t, l.Identity.PublicKey, MustBytesFromHex(t, "048bef2231e64d5c7147bd4b8afb84abd4126ee8d8335e4b069ac0a65c7be711cea5c1b8d47bc20ebaecdca588600ddf2894675e78b2ef17cf49e7bbaf98080361"))
		require.Equal(t, l.Identity.Signatures.ID, MustBytesFromHex(t, "3045022100f5f6f10571d14347aaf34e526ce3419fd64d75ffa7aa73692cbb6aeb6fbc147102203a3e3fa41fa8fcbb9fc7c148af5b640e2f704b20b3a4e0b93fc3a6d44dffb41e"))
		require.Equal(t, l.Identity.Signatures.PublicKey, MustBytesFromHex(t, "3044022020982b8492be0c184dc29de0a3a3bd86a86ba997756b0bf41ddabd24b47c5acf02203745fda39d7df650a5a478e52bbe879f0cb45c074025a93471414a56077640a4"))
	})

	t.Run("has the correct public key", func(t *testing.T) {
		l, err := ipfslog.NewLog(dag, identities[0], &ipfslog.LogOptions{ID: "A"})
		require.NoError(t, err)
		require.NotNil(t, l.ID)
		require.Equal(t, l.Identity.PublicKey, identities[0].PublicKey)
	})

	t.Run("has the correct pkSignature", func(t *testing.T) {
		l, err := ipfslog.NewLog(dag, identities[0], &ipfslog.LogOptions{ID: "A"})
		require.NoError(t, err)
		require.NotNil(t, l.ID)
		require.Equal(t, l.Identity.Signatures.ID, identities[0].Signatures.ID)
	})

	t.Run("has the correct signature", func(t *testing.T) {
		l, err := ipfslog.NewLog(dag, identities[0], &ipfslog.LogOptions{ID: "A"})
		require.NoError(t, err)
		require.NotNil(t, l.ID)
		require.Equal(t, l.Identity.Signatures.PublicKey, identities[0].Signatures.PublicKey)
	})

	//////////////

	t.Run("entries contain an identity", func(t *testing.T) {
		l, err := ipfslog.NewLog(dag, identities[0], &ipfslog.LogOptions{ID: "A"})
		require.NoError(t, err)
		require.NotNil(t, l.ID)

		_, err = l.Append(ctx, []byte("one"), nil)
		require.NoError(t, err)
		require.NotNil(t, l.ID)

		require.NotNil(t, l.Values().At(0).GetSig())
		require.Equal(t, l.Values().At(0).GetIdentity().Filtered(), identities[0].Filtered())
	})

	t.Run("doesn't sign entries when identity is not defined", func(t *testing.T) {
		_, err := ipfslog.NewLog(dag, nil, nil)
		require.Error(t, err)
		require.Equal(t, err, errmsg.ErrIdentityNotDefined)
	})

	t.Run("doesn't join logs with different IDs", func(t *testing.T) {
		l1, err := ipfslog.NewLog(dag, identities[0], &ipfslog.LogOptions{ID: "A"})
		require.NoError(t, err)
		require.NotNil(t, l1.ID)

		l2, err := ipfslog.NewLog(dag, identities[0], &ipfslog.LogOptions{ID: "B"})
		require.NoError(t, err)
		require.NotNil(t, l2.ID)

		_, err = l1.Append(ctx, []byte("one"), nil)
		require.NoError(t, err)

		_, err = l2.Append(ctx, []byte("two"), nil)
		require.NoError(t, err)

		_, err = l2.Append(ctx, []byte("three"), nil)
		require.NoError(t, err)

		_, err = l1.Join(l2, -1)
		require.NoError(t, err)

		require.Equal(t, l1.ID, "A")
		require.Equal(t, l1.Values().Len(), 1)
		require.Equal(t, l1.Values().At(0).GetPayload(), []byte("one"))
	})

	t.Run("throws an error if log is signed but trying to merge with an entry that doesn't have public signing key", func(t *testing.T) {
		l1, err := ipfslog.NewLog(dag, identities[0], &ipfslog.LogOptions{ID: "A"})
		require.NoError(t, err)
		require.NotNil(t, l1.ID)

		l2, err := ipfslog.NewLog(dag, identities[1], &ipfslog.LogOptions{ID: "A"})
		require.NoError(t, err)
		require.NotNil(t, l2.ID)

		_, err = l1.Append(ctx, []byte("one"), nil)
		require.NoError(t, err)

		_, err = l2.Append(ctx, []byte("two"), nil)
		require.NoError(t, err)

		l2.Values().At(0).SetKey(nil)

		_, err = l1.Join(l2, -1)
		require.Error(t, err)
		require.Contains(t, err.Error(), errmsg.ErrKeyNotDefined.Error())
	})

	t.Run("throws an error if log is signed but trying to merge an entry that doesn't have a signature", func(t *testing.T) {
		l1, err := ipfslog.NewLog(dag, identities[0], &ipfslog.LogOptions{ID: "A"})
		require.NoError(t, err)
		require.NotNil(t, l1.ID)

		l2, err := ipfslog.NewLog(dag, identities[1], &ipfslog.LogOptions{ID: "A"})
		require.NoError(t, err)
		require.NotNil(t, l2.ID)

		_, err = l1.Append(ctx, []byte("one"), nil)
		require.NoError(t, err)

		_, err = l2.Append(ctx, []byte("two"), nil)
		require.NoError(t, err)

		l2.Values().At(0).SetSig(nil)

		_, err = l1.Join(l2, -1)
		require.Error(t, err)
		require.Contains(t, err.Error(), errmsg.ErrSigNotDefined.Error())
	})

	t.Run("throws an error if log is signed but the signature doesn't verify", func(t *testing.T) {
		l1, err := ipfslog.NewLog(dag, identities[0], &ipfslog.LogOptions{ID: "A"})
		require.NoError(t, err)
		require.NotNil(t, l1.ID)

		l2, err := ipfslog.NewLog(dag, identities[1], &ipfslog.LogOptions{ID: "A"})
		require.NoError(t, err)
		require.NotNil(t, l2.ID)

		_, err = l1.Append(ctx, []byte("one"), nil)
		require.NoError(t, err)

		_, err = l2.Append(ctx, []byte("two"), nil)
		require.NoError(t, err)

		l2.Values().At(0).SetSig(l1.Values().At(0).GetSig())

		_, err = l1.Join(l2, -1)
		require.Error(t, err)
		require.Contains(t, err.Error(), errmsg.ErrSigNotVerified.Error())

		require.Equal(t, l1.Values().Len(), 1)
		require.Equal(t, l1.Values().At(0).GetPayload(), []byte("one"))
	})

	t.Run("throws an error if entry doesn't have append access", func(t *testing.T) {
		l1, err := ipfslog.NewLog(dag, identities[0], &ipfslog.LogOptions{ID: "A"})
		require.NoError(t, err)
		require.NotNil(t, l1.ID)

		l2, err := ipfslog.NewLog(dag, identities[1], &ipfslog.LogOptions{ID: "A", AccessController: &DenyAll{}})
		require.NoError(t, err)
		require.NotNil(t, l2.ID)

		_, err = l1.Append(ctx, []byte("one"), nil)
		require.NoError(t, err)

		_, err = l2.Append(ctx, []byte("two"), nil)
		require.Error(t, err)
		require.Contains(t, err.Error(), errmsg.ErrLogAppendDenied.Error())
	})

	t.Run("throws an error upon join if entry doesn't have append access", func(t *testing.T) {
		l1, err := ipfslog.NewLog(dag, identities[0], &ipfslog.LogOptions{ID: "A", AccessController: &TestACL{refIdentity: identities[1]}})
		require.NoError(t, err)
		require.NotNil(t, l1.ID)

		l2, err := ipfslog.NewLog(dag, identities[1], &ipfslog.LogOptions{ID: "A"})
		require.NoError(t, err)
		require.NotNil(t, l2.ID)

		_, err = l1.Append(ctx, []byte("one"), nil)
		require.NoError(t, err)

		_, err = l2.Append(ctx, []byte("two"), nil)
		require.NoError(t, err)

		_, err = l1.Join(l2, -1)
		require.Error(t, err)
		require.Contains(t, err.Error(), errmsg.ErrLogJoinFailed.Error())
	})
}
