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
var identityKeysPath = "./orbitdb/identity/identitykeys"

type Identities struct {
	keyStore keystore.Interface
}

func GetHandlerFor(typeName string) (func(*CreateIdentityOptions) Interface, error) {
	if !IsSupported(typeName) {
		return nil, errors.New(fmt.Sprintf("IdentityProvider type '%s' is not supported", typeName))
	}

	return supportedTypes[typeName], nil
}

func NewIdentities(keyStore keystore.Interface) *Identities {
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

func (i *Identities) Verify(signature []byte, publicKey crypto.PubKey, data []byte) (bool, error) {
	return publicKey.Verify(data, signature)
}

type MigrateOptions struct {
	TargetPath string
	TargetId   string
}

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

func (i *Identities) CreateIdentity(options *CreateIdentityOptions) (*Identity, error) {
	NewIdentityProvider, err := GetHandlerFor(options.Type)
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
	//	if err := options.Migrate(&MigrateOptions{ TargetPath: i.keyStore.Path, TargetId: id }); err != nil {
	//		return nil, err
	//	}
	//}

	publicKey, idSignature, err := i.SignID(id)
	if err != nil {
		return nil, err
	}

	publicKeyBytes, err := publicKey.Raw()
	if err != nil {
		return nil, err
	}

	publicKeyBytes, err = compressedToUncompressedS256Key(publicKeyBytes)
	if err != nil {
		return nil, err
	}

	pubKeyIdSignature, err := identityProvider.SignIdentity(append(publicKeyBytes, idSignature...), options.ID)
	if err != nil {
		return nil, err
	}

	return &Identity{
		ID:        id,
		PublicKey: publicKeyBytes,
		Signatures: &IdentitySignature{
			ID:        idSignature,
			PublicKey: pubKeyIdSignature,
		},
		Type:     identityProvider.GetType(),
		Provider: identityProvider,
	}, nil
}

func (i *Identities) SignID(id string) (crypto.PubKey, []byte, error) {
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

func (i *Identities) VerifyIdentity(identity *Identity) error {
	pubKey, err := identity.GetPublicKey()
	if err != nil {
		return err
	}

	idBytes, err := hex.DecodeString(identity.ID)

	err = i.keyStore.Verify(
		identity.Signatures.ID,
		pubKey,
		idBytes,
	)

	if err != nil {
		return err
	}

	return VerifyIdentity(identity)
}

func VerifyIdentity(identity *Identity) error {
	identityProvider, err := GetHandlerFor(identity.Type)
	if err != nil {
		return err
	}

	return identityProvider(nil).VerifyIdentity(identity)
}

func CreateIdentity(options *CreateIdentityOptions) (*Identity, error) {
	ks := options.Keystore
	if ks == nil {
		return nil, errors.New("a keystore is required")
	}

	identities := NewIdentities(ks)

	return identities.CreateIdentity(options)
}

func IsSupported(typeName string) bool {
	_, ok := supportedTypes[typeName]

	return ok
}

func AddIdentityProvider(identityProvider func(*CreateIdentityOptions) Interface) error {
	if identityProvider == nil {
		return errors.New("IdentityProvider class needs to be given as an option")
	}

	supportedTypes[identityProvider(nil).GetType()] = identityProvider

	return nil
}

func RemoveIdentityProvider(name string) {
	delete(supportedTypes, name)
}
