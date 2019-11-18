package identityprovider // import "berty.tech/go-ipfs-log/identityprovider"

import (
	"encoding/hex"
	"fmt"

	"berty.tech/go-ipfs-log/keystore"
	"github.com/btcsuite/btcd/btcec"
	crypto "github.com/libp2p/go-libp2p-crypto"
	"github.com/pkg/errors"
)

var supportedTypes = map[string]func(*CreateIdentityOptions) Interface{
	"orbitdb": NewOrbitDBIdentityProvider,
}

type Identities struct {
	keyStore keystore.Interface
}

func getHandlerFor(typeName string) (func(*CreateIdentityOptions) Interface, error) {
	if !IsSupported(typeName) {
		return nil, errors.New(fmt.Sprintf("IdentityProvider type '%s' is not supported", typeName))
	}

	return supportedTypes[typeName], nil
}

func newIdentities(keyStore keystore.Interface) *Identities {
	return &Identities{
		keyStore: keyStore,
	}
}

func (i *Identities) Sign(identity *Identity, data []byte) ([]byte, error) {
	privKey, err := i.keyStore.GetKey(identity.ID)
	if err != nil {
		return nil, err
	}

	sig, err := i.keyStore.Sign(privKey, data)
	if err != nil {
		return nil, err
	}

	return sig, nil
}

// Verify checks a signature.
func (i *Identities) Verify(signature []byte, publicKey crypto.PubKey, data []byte) (bool, error) {
	// TODO: Check why this is related to an identity
	return publicKey.Verify(data, signature)
}

//type MigrateOptions struct {
//	TargetPath string
//	TargetID   string
//}

func compressedToUncompressedS256Key(pubKeyBytes []byte) ([]byte, error) {
	pubKey, err := btcec.ParsePubKey(pubKeyBytes, btcec.S256())
	if err != nil {
		return nil, err
	}

	if !btcec.IsCompressedPubKey(pubKeyBytes) {
		return pubKeyBytes, nil
	}

	return pubKey.SerializeUncompressed(), nil
}

// CreateIdentity creates a new Identity.
func (i *Identities) CreateIdentity(options *CreateIdentityOptions) (*Identity, error) {
	NewIdentityProvider, err := getHandlerFor(options.Type)
	if err != nil {
		return nil, err
	}

	identityProvider := NewIdentityProvider(options)
	id, err := identityProvider.GetID(options)
	if err != nil {
		return nil, err
	}

	// FIXME ?
	//if options.Migrate != nil {
	//	if err := options.Migrate(&MigrateOptions{ TargetPath: i.keyStore.Path, TargetID: id }); err != nil {
	//		return nil, err
	//	}
	//}

	publicKey, idSignature, err := i.signID(id)
	if err != nil {
		return nil, err
	}

	publicKeyBytes, err := publicKey.Raw()
	if err != nil {
		return nil, err
	}

	// JS version of IPFS Log expects an uncompressed Secp256k1 key
	if publicKey.Type().String() == "Secp256k1" {
		publicKeyBytes, err = compressedToUncompressedS256Key(publicKeyBytes)
		if err != nil {
			return nil, err
		}
	}

	pubKeyIDSignature, err := identityProvider.SignIdentity(append(publicKeyBytes, idSignature...), options.ID)
	if err != nil {
		return nil, err
	}

	return &Identity{
		ID:        id,
		PublicKey: publicKeyBytes,
		Signatures: &IdentitySignature{
			ID:        idSignature,
			PublicKey: pubKeyIDSignature,
		},
		Type:     identityProvider.GetType(),
		Provider: identityProvider,
	}, nil
}

func (i *Identities) signID(id string) (crypto.PubKey, []byte, error) {
	privKey, err := i.keyStore.GetKey(id)
	if err != nil {
		privKey, err = i.keyStore.CreateKey(id)

		if err != nil {
			return nil, nil, err
		}
	}

	idSignature, err := i.keyStore.Sign(privKey, []byte(id))
	if err != nil {
		return nil, nil, err
	}

	return privKey.GetPublic(), idSignature, nil
}

// VerifyIdentity checks an identity.
func (i *Identities) VerifyIdentity(identity *Identity) error {
	pubKey, err := identity.GetPublicKey()
	if err != nil {
		return err
	}

	idBytes, err := hex.DecodeString(identity.ID)
	if err != nil {
		return err
	}

	err = i.keyStore.Verify(
		identity.Signatures.ID,
		pubKey,
		idBytes,
	)
	if err != nil {
		return err
	}

	identityProvider, err := getHandlerFor(identity.Type)
	if err != nil {
		return err
	}

	return identityProvider(nil).VerifyIdentity(identity)
}

// CreateIdentity creates a new identity.
func CreateIdentity(options *CreateIdentityOptions) (*Identity, error) {
	ks := options.Keystore
	if ks == nil {
		return nil, errors.New("a keystore is required")
	}

	identities := newIdentities(ks)

	return identities.CreateIdentity(options)
}

// IsSupported checks if an identity type is supported.
func IsSupported(typeName string) bool {
	_, ok := supportedTypes[typeName]

	return ok
}

// AddIdentityProvider registers an new identity provider.
func AddIdentityProvider(identityProvider func(*CreateIdentityOptions) Interface) error {
	if identityProvider == nil {
		return errors.New("'IdentityProvider' class needs to be given as an option")
	}

	supportedTypes[identityProvider(nil).GetType()] = identityProvider

	return nil
}

// RemoveIdentityProvider unregisters an identity provider.
func RemoveIdentityProvider(name string) {
	delete(supportedTypes, name)
}
