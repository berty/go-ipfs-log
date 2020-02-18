package test

import (
	"encoding/hex"
	"testing"

	"github.com/ipfs/go-cid"
	ds "github.com/ipfs/go-datastore"
	"github.com/multiformats/go-multibase"
)

func NewIdentityDataStore(t testing.TB) ds.Datastore {
	t.Helper()

	var identityKeys = map[string][]byte{
		"userA": MustBytesFromHex(t, "0a135ce157a9ccb8375c2fae0d472f1eade4b40b37704c02df923b78ca03c627"),
		"userB": MustBytesFromHex(t, "855f70d3b5224e5af76c23db0792339ca8d968a5a802ff0c5b54d674ef01aaad"),
		"userC": MustBytesFromHex(t, "291d4dc915d81e9ebe5627c3f5e7309e819e721ee75e63286baa913497d61c78"),
		"userD": MustBytesFromHex(t, "faa2d697318a6f8daeb8f4189fc657e7ae1b24e18c91c3bb9b95ad3c0cc050f8"),
		"02a38336e3a47f545a172c9f77674525471ebeda7d6c86140e7a778f67ded92260": MustBytesFromHex(t, "7c6140e9ae4c70eb11600b3d550cc6aac45511b5a660f4e75fe9a7c4e6d1c7b7"),
		"03e0480538c2a39951d054e17ff31fde487cb1031d0044a037b53ad2e028a3e77c": MustBytesFromHex(t, "97f64ca2bf7bd6aa2136eb0aa3ce512433bd903b91d48b2208052d6ff286d080"),
		"032f7b6ef0432b572b45fcaf27e7f6757cd4123ff5c5266365bec82129b8c5f214": MustBytesFromHex(t, "2b487a932233c8691024c951faaeac207be161797bdda7bd934c0125012a5551"),
		"0358df8eb5def772917748fdf8a8b146581ad2041eae48d66cc6865f11783499a6": MustBytesFromHex(t, "1cd65d23d72932f5ca2328988d19a5b11fbab1f4c921ef2471768f1773bd56de"),
	}

	dataStore := ds.NewMapDatastore()
	for k, v := range identityKeys {
		err := dataStore.Put(ds.NewKey(k), v)
		if err != nil {
			t.Fatal(err)
		}
	}

	return dataStore
}

func CidB32(t testing.TB, b58CID string) string {
	t.Helper()

	c, err := cid.Parse(b58CID)
	if err != nil {
		t.Fatal(err)
	}

	return c.Encode(multibase.MustNewEncoder(multibase.Base32))
}

func MustCID(t testing.TB, s string) cid.Cid {
	t.Helper()

	c, err := cid.Decode(s)

	if err != nil {
		t.Fatal(err)
	}

	return c
}
func MustBytesFromHex(t testing.TB, s string) []byte {
	t.Helper()

	b, err := hex.DecodeString(s)
	if err != nil {
		t.Fatal(err)
	}

	return b
}
