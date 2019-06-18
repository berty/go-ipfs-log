package identityprovider // import "berty.tech/go-ipfs-log/identityprovider"

import "berty.tech/go-ipfs-log/keystore"

type CreateIdentityOptions struct {
	IdentityKeysPath string
	Type             string
	Keystore         keystore.Interface
	//Migrate          func(*MigrateOptions) error
	ID string
}

type Interface interface {
	// GetID returns id of identity (to be signed by orbit-db public key).
	GetID(*CreateIdentityOptions) (string, error)

	// SignIdentity returns signature of OrbitDB public key signature.
	SignIdentity(data []byte, id string) ([]byte, error)

	// GetType returns the type for this identity provider.
	GetType() string

	// VerifyIdentity checks an identity.
	VerifyIdentity(identity *Identity) error

	// Sign will sign a value.
	Sign(identity *Identity, bytes []byte) ([]byte, error)
}
