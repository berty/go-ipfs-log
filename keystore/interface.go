package keystore // import "berty.tech/go-ipfs-log/keystore"

import crypto "github.com/libp2p/go-libp2p-core/crypto"

type Keystore interface {
	// HasKey checks whether a given key ID exist in the keystore.
	HasKey(id string) (bool, error)

	// CreateKey creates a new key in the key store.
	CreateKey(id string) (crypto.PrivKey, error)

	// GetKey retrieves a key from the keystore.
	GetKey(id string) (crypto.PrivKey, error)

	// Sign signs a value using a given private key.
	Sign(pubKey crypto.PrivKey, bytes []byte) ([]byte, error)

	// Verify verifies a signature.
	Verify(signature []byte, publicKey crypto.PubKey, data []byte) error
}
