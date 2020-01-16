package keystore

import (
	"crypto/rand"
	"encoding/base64"

	lru "github.com/hashicorp/golang-lru"
	datastore "github.com/ipfs/go-datastore"
	crypto "github.com/libp2p/go-libp2p-core/crypto"
	"github.com/pkg/errors"
)

// New creates a new keystore.
func New(store datastore.Datastore) (Keystore, error) {
	cache, err := lru.New(128)
	if err != nil {
		return nil, err
	}

	ks := &keystore{store: store, cache: cache}
	return ks, nil
}

type keystore struct {
	store datastore.Datastore
	cache *lru.Cache
}

func (k *keystore) Sign(privKey crypto.PrivKey, bytes []byte) ([]byte, error) {
	return privKey.Sign(bytes)
}

func (k *keystore) Verify(signature []byte, publicKey crypto.PubKey, data []byte) error {
	ok, err := publicKey.Verify(data, signature)
	if err != nil {
		return err
	}

	if !ok {
		return errors.New("signature is not valid for the supplied data")
	}

	return nil
}

func (k *keystore) HasKey(id string) (bool, error) {
	storedKey, ok := k.cache.Peek(id)

	if ok == false {
		value, err := k.store.Get(datastore.NewKey(id))
		if err != nil {
			return false, err
		}

		if storedKey != nil {
			k.cache.Add(id, base64.StdEncoding.EncodeToString(value))
		}
	}

	return storedKey != nil, nil
}

func (k *keystore) CreateKey(id string) (crypto.PrivKey, error) {
	// FIXME: I kept Secp256k1 for compatibility with OrbitDB, should we change this?
	priv, _, err := crypto.GenerateSecp256k1Key(rand.Reader)
	if err != nil {
		return nil, err
	}

	keyBytes, err := priv.Raw()
	if err != nil {
		return nil, err
	}

	if err := k.store.Put(datastore.NewKey(id), keyBytes); err != nil {
		return nil, err
	}

	k.cache.Add(id, base64.StdEncoding.EncodeToString(keyBytes))

	return priv, nil
}

func (k *keystore) GetKey(id string) (crypto.PrivKey, error) {
	var err error
	var keyBytes []byte

	cachedKey, ok := k.cache.Get(id)
	if !ok || cachedKey == nil {
		keyBytes, err = k.store.Get(datastore.NewKey(id))

		if err != nil {
			return nil, errors.Wrap(err, "unable to fetch a private key from keystore")
		}
		k.cache.Add(id, base64.StdEncoding.EncodeToString(keyBytes))
	} else {
		keyBytes, err = base64.StdEncoding.DecodeString(cachedKey.(string))
		if err != nil {
			return nil, errors.Wrap(err, "unable to cast private key to bytes")
		}
	}

	privateKey, err := crypto.UnmarshalSecp256k1PrivateKey(keyBytes)
	if err != nil {
		return nil, err
	}

	return privateKey, nil
}

var _ Keystore = &keystore{}
