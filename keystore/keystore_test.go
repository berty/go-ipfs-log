package keystore

import (
	"crypto/rand"
	"testing"

	ccrypto "github.com/libp2p/go-libp2p-core/crypto"

	"github.com/ipfs/go-datastore"
)

func TestSignAndVerify(t *testing.T) {
	const Message = "Test Message"
	ds := datastore.NewNullDatastore()
	ks, err := NewKeystore(ds)
	if err != nil {
		t.Error(err)
	}
	priv, pub, _ := ccrypto.GenerateRSAKeyPair(2048, rand.Reader)
	signed, err := ks.Sign(priv, []byte(Message))
	if err != nil {
		t.Error(err)
	}

	if err := ks.Verify(signed, pub, []byte(Message)); err != nil {
		t.Error()
	}
}

func TestHasKey(t *testing.T) {
	ds := datastore.NewNullDatastore()
	ks, err := NewKeystore(ds)
	if err != nil {
		t.Error(err)
	}
	_, err = ks.CreateKey("test_key_1")
	if err != nil {
		t.Error(err)
	}

	exists, err := ks.HasKey("test_key_1")
	if err != nil {
		t.Error(err)
	}
	if !exists {
		t.Errorf("failed to find key in cache")
	}

	_, err = ks.CreateKey("test_key_2")
	if err != nil {
		t.Error(err)
	}
	ks.cache.Remove("test_key_2")

	exists, _ = ks.HasKey("test_key_2")
	if exists {
		t.Errorf("should fail key lookup in cache")
	}

}

func TestGetKey(t *testing.T) {
	ds := datastore.NewNullDatastore()
	ks, err := NewKeystore(ds)
	if err != nil {
		t.Error(err)
	}
	created, err := ks.CreateKey("test_key_1")
	if err != nil {
		t.Error(err)
	}

	fetched, err := ks.GetKey("test_key_1")
	if err != nil {
		t.Error(err)
	}

	if !fetched.Equals(created) {
		t.Errorf("created key does not match fetched key from store")
	}

}
