package identity

import (
	"encoding/hex"

	"berty.tech/go-ipfs-log/keystore"
	crypto "github.com/libp2p/go-libp2p-core/crypto"
)

// Verify checks a signature.
func Verify(signature []byte, publicKey crypto.PubKey, data []byte) (bool, error) {
	// TODO: Check why this is related to an identity
	return publicKey.Verify(data, signature)
}

func Sign(ks keystore.Keystore, identity *Identity, data []byte) ([]byte, error) {
	privKey, err := ks.GetKey(identity.ID)
	if err != nil {
		return nil, err
	}

	sig, err := ks.Sign(privKey, data)
	if err != nil {
		return nil, err
	}

	return sig, nil
}

// CreateIdentity creates a new identity.
func CreateIdentity(ks keystore.Keystore, id string) (*Identity, error) {
	provider := NewOrbitDBProvider(ks)
	id, err := provider.GetID(id)
	if err != nil {
		return nil, err
	}

	// FIXME ?
	//if options.Migrate != nil {
	//	if err := options.Migrate(&MigrateOptions{ TargetPath: ks.Path, TargetID: id }); err != nil {
	//		return nil, err
	//	}
	//}

	publicKey, idSignature, err := signID(ks, id)
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

	pubKeyIDSignature, err := provider.SignIdentity(append(publicKeyBytes, idSignature...), id)
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
		Type:     provider.GetType(),
		Provider: provider,
	}, nil
}

func signID(ks keystore.Keystore, id string) (crypto.PubKey, []byte, error) {
	privKey, err := ks.GetKey(id)
	if err != nil {
		privKey, err = ks.CreateKey(id)

		if err != nil {
			return nil, nil, err
		}
	}

	idSignature, err := ks.Sign(privKey, []byte(id))
	if err != nil {
		return nil, nil, err
	}

	return privKey.GetPublic(), idSignature, nil
}

// VerifyIdentity checks an identity.
func VerifyIdentity(ks keystore.Keystore, identity *Identity) error {
	pubKey, err := identity.GetPublicKey()
	if err != nil {
		return err
	}

	idBytes, err := hex.DecodeString(identity.ID)
	if err != nil {
		return err
	}

	err = ks.Verify(
		identity.Signatures.ID,
		pubKey,
		idBytes,
	)
	if err != nil {
		return err
	}

	provider := NewOrbitDBProvider(ks)
	return provider.VerifyIdentity(identity)
}
