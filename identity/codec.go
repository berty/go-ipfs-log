package identity

import "github.com/btcsuite/btcd/btcec"

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
