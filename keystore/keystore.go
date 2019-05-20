package keystore

import (
	"crypto/rand"
	"github.com/hashicorp/golang-lru"
	"github.com/ipfs/go-datastore"
	"github.com/libp2p/go-libp2p-crypto"
	"github.com/pkg/errors"
)

type Keystore struct {
	store datastore.Datastore
	cache *lru.Cache
}

func NewKeystore(store datastore.Datastore) (*Keystore, error) {
	cache, err := lru.New(128)
	if err != nil {
		return nil, err
	}

	return &Keystore{
		store: store,
		cache: cache,
	}, nil
}

func (k *Keystore) HasKey(id string) (bool, error) {
	var err error

	hasKey := false
	storedKey, ok := k.cache.Peek(id)

	if ok == false {
		storedKey, err = k.store.Get(datastore.NewKey(id))
		if err != nil {
			return false, err
		}

		if storedKey != nil {
			k.cache.Add(id, storedKey)
		}
	}

	hasKey = storedKey != nil

	return hasKey, nil
}

func (k *Keystore) CreateKey(id string) (*crypto.Secp256k1PrivateKey, error) {
	// FIXME: I kept Secp256k1 for compatibility with OrbitDB, should we change this?
	priv, _, err := crypto.GenerateSecp256k1Key(rand.Reader)
	if err != nil {
		return nil, err
	}

	keyBytes, err := crypto.MarshalPrivateKey(priv)
	if err != nil {
		return nil, err
	}

	if err := k.store.Put(datastore.NewKey(id), keyBytes); err != nil {
		return nil, err
	}

	k.cache.Add(id, priv)

	return priv.(*crypto.Secp256k1PrivateKey), nil
}

func (k *Keystore) GetKey(id string) (*crypto.Secp256k1PrivateKey, error) {
	var err error

	cachedKey, ok := k.cache.Get(id)
	if !ok || cachedKey == nil {
		cachedKey, err = k.store.Get(datastore.NewKey(id))

		if err != nil {
			cachedKey = nil
		} else {
			k.cache.Add(id, cachedKey)
		}
	}

	cachedKeyBytes, ok := cachedKey.([]byte)
	if !ok {
		return nil, errors.New("unable to cast private key to bytes")
	}

	privateKey, err := crypto.UnmarshalPrivateKey(cachedKeyBytes)
	if err != nil {
		return nil, err
	}

	return privateKey.(*crypto.Secp256k1PrivateKey), nil
}

var _ Interface = &Keystore{}
