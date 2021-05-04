package test

import (
	"testing"

	"github.com/ipfs/go-cid"

	"berty.tech/go-ipfs-log/entry"
	"berty.tech/go-ipfs-log/identityprovider"
)

func getEntriesV0Fixtures(t *testing.T) map[string]*entry.Entry {
	return map[string]*entry.Entry{
		"hello": {
			Hash:    MustCID(t, "Qmc2DEiLirMH73kHpuFPbt3V65sBrnDWkJYSjUQHXXvghT"),
			LogID:   "A",
			Payload: []byte("hello"),
			V:       0,
			Clock:   entry.NewLamportClock(MustBytesFromHex(t, "0411a0d38181c9374eca3e480ecada96b1a4db9375c5e08c3991557759d22f6f2f902d0dc5364a948035002504d825308b0c257b7cbb35229c2076532531f8f4ef"), 0),
			Sig:     MustBytesFromHex(t, "3044022062f4cfc8b8f3cc01283b25eab3eeb295614bb0faa8bd20f026c1487ae663121102207ce415bd7423b66d695338c17122e937259f77d1e86494d3146436f0959fccc6"),
			Key:     MustBytesFromHex(t, "0411a0d38181c9374eca3e480ecada96b1a4db9375c5e08c3991557759d22f6f2f902d0dc5364a948035002504d825308b0c257b7cbb35229c2076532531f8f4ef"),
			Next:    []cid.Cid{},
		},
		"helloWorld": {
			Hash:    MustCID(t, "QmUKMoRrmsYAzQg1nQiD7Fzgpo24zXky7jVJNcZGiSAdhc"),
			LogID:   "A",
			Payload: []byte("hello world"),
			V:       0,
			Clock:   entry.NewLamportClock(MustBytesFromHex(t, "0411a0d38181c9374eca3e480ecada96b1a4db9375c5e08c3991557759d22f6f2f902d0dc5364a948035002504d825308b0c257b7cbb35229c2076532531f8f4ef"), 0),
			Sig:     MustBytesFromHex(t, "3044022062f4cfc8b8f3cc01283b25eab3eeb295614bb0faa8bd20f026c1487ae663121102207ce415bd7423b66d695338c17122e937259f77d1e86494d3146436f0959fccc6"),
			Key:     MustBytesFromHex(t, "0411a0d38181c9374eca3e480ecada96b1a4db9375c5e08c3991557759d22f6f2f902d0dc5364a948035002504d825308b0c257b7cbb35229c2076532531f8f4ef"),
			Next:    []cid.Cid{},
		},
		"helloAgain": {
			Hash:    MustCID(t, "QmZ8va2fSjRufV1sD6x5mwi6E5GrSjXHx7RiKFVBzkiUNZ"),
			LogID:   "A",
			Payload: []byte("hello again"),
			V:       0,
			Clock:   entry.NewLamportClock(MustBytesFromHex(t, "0411a0d38181c9374eca3e480ecada96b1a4db9375c5e08c3991557759d22f6f2f902d0dc5364a948035002504d825308b0c257b7cbb35229c2076532531f8f4ef"), 0),
			Sig:     MustBytesFromHex(t, "3044022062f4cfc8b8f3cc01283b25eab3eeb295614bb0faa8bd20f026c1487ae663121102207ce415bd7423b66d695338c17122e937259f77d1e86494d3146436f0959fccc6"),
			Key:     MustBytesFromHex(t, "0411a0d38181c9374eca3e480ecada96b1a4db9375c5e08c3991557759d22f6f2f902d0dc5364a948035002504d825308b0c257b7cbb35229c2076532531f8f4ef"),
			Next:    []cid.Cid{MustCID(t, "QmUKMoRrmsYAzQg1nQiD7Fzgpo24zXky7jVJNcZGiSAdhc")},
		},
	}
}

func getEntriesV1Fixtures(t *testing.T, id *identityprovider.Identity) []entry.Entry {
	return []entry.Entry{
		{
			Payload: []byte("one"),
			LogID:   "A",
			Next:    []cid.Cid{},
			V:       1,
			Key:     MustBytesFromHex(t, "048bef2231e64d5c7147bd4b8afb84abd4126ee8d8335e4b069ac0a65c7be711cea5c1b8d47bc20ebaecdca588600ddf2894675e78b2ef17cf49e7bbaf98080361"),
			Sig:     MustBytesFromHex(t, "3045022100f72546c99cf30eda1d394d91209bdb4569408a792caf9dc7c6415fef37a3118d0220645c4a6d218f8fc478af5bab175aaa99e1505d70c2a00997aacafa8de697944e"),
			Identity: &identityprovider.Identity{
				ID:        "03e0480538c2a39951d054e17ff31fde487cb1031d0044a037b53ad2e028a3e77c",
				PublicKey: MustBytesFromHex(t, "048bef2231e64d5c7147bd4b8afb84abd4126ee8d8335e4b069ac0a65c7be711cea5c1b8d47bc20ebaecdca588600ddf2894675e78b2ef17cf49e7bbaf98080361"),
				Signatures: &identityprovider.IdentitySignature{
					ID:        MustBytesFromHex(t, "3045022100f5f6f10571d14347aaf34e526ce3419fd64d75ffa7aa73692cbb6aeb6fbc147102203a3e3fa41fa8fcbb9fc7c148af5b640e2f704b20b3a4e0b93fc3a6d44dffb41e"),
					PublicKey: MustBytesFromHex(t, "3044022020982b8492be0c184dc29de0a3a3bd86a86ba997756b0bf41ddabd24b47c5acf02203745fda39d7df650a5a478e52bbe879f0cb45c074025a93471414a56077640a4"),
				},
				Type:     "orbitdb",
				Provider: id.Provider,
			},
			Hash:  MustCID(t, "zdpuAsJDrLKrAiU8M518eu6mgv9HzS3e1pfH5XC7LUsFgsK5c"),
			Clock: entry.NewLamportClock(MustBytesFromHex(t, "048bef2231e64d5c7147bd4b8afb84abd4126ee8d8335e4b069ac0a65c7be711cea5c1b8d47bc20ebaecdca588600ddf2894675e78b2ef17cf49e7bbaf98080361"), 1),
		},

		{
			Payload: []byte("two"),
			LogID:   "A",
			Next:    []cid.Cid{MustCID(t, "zdpuAsJDrLKrAiU8M518eu6mgv9HzS3e1pfH5XC7LUsFgsK5c")},
			V:       1,
			Key:     MustBytesFromHex(t, "048bef2231e64d5c7147bd4b8afb84abd4126ee8d8335e4b069ac0a65c7be711cea5c1b8d47bc20ebaecdca588600ddf2894675e78b2ef17cf49e7bbaf98080361"),
			Sig:     MustBytesFromHex(t, "3045022100b85c85c59e6d0952f95e3839e48b43b4073ef26f6f4696d785ce64053cd5869a0220644a4a7a15ddcd2b152611b08bf23b9df7823846719f2d0e4b0aff64190ed146"),
			Identity: &identityprovider.Identity{
				ID:        "03e0480538c2a39951d054e17ff31fde487cb1031d0044a037b53ad2e028a3e77c",
				PublicKey: MustBytesFromHex(t, "048bef2231e64d5c7147bd4b8afb84abd4126ee8d8335e4b069ac0a65c7be711cea5c1b8d47bc20ebaecdca588600ddf2894675e78b2ef17cf49e7bbaf98080361"),
				Signatures: &identityprovider.IdentitySignature{
					ID:        MustBytesFromHex(t, "3045022100f5f6f10571d14347aaf34e526ce3419fd64d75ffa7aa73692cbb6aeb6fbc147102203a3e3fa41fa8fcbb9fc7c148af5b640e2f704b20b3a4e0b93fc3a6d44dffb41e"),
					PublicKey: MustBytesFromHex(t, "3044022020982b8492be0c184dc29de0a3a3bd86a86ba997756b0bf41ddabd24b47c5acf02203745fda39d7df650a5a478e52bbe879f0cb45c074025a93471414a56077640a4"),
				},
				Type:     "orbitdb",
				Provider: id.Provider,
			},
			Hash:  MustCID(t, "zdpuAxgKyiM9qkP9yPKCCqrHer9kCqYyr7KbhucsPwwfh6JB3"),
			Clock: entry.NewLamportClock(MustBytesFromHex(t, "048bef2231e64d5c7147bd4b8afb84abd4126ee8d8335e4b069ac0a65c7be711cea5c1b8d47bc20ebaecdca588600ddf2894675e78b2ef17cf49e7bbaf98080361"), 2),
		},

		{
			Payload: []byte("three"),
			LogID:   "A",
			Next:    []cid.Cid{MustCID(t, "zdpuAxgKyiM9qkP9yPKCCqrHer9kCqYyr7KbhucsPwwfh6JB3")},
			V:       1,
			Key:     MustBytesFromHex(t, "048bef2231e64d5c7147bd4b8afb84abd4126ee8d8335e4b069ac0a65c7be711cea5c1b8d47bc20ebaecdca588600ddf2894675e78b2ef17cf49e7bbaf98080361"),
			Sig:     MustBytesFromHex(t, "304402206f6a1582bc2c18b63eeb5b1e2280f2700c5d467d60185738702f90f4e655214602202ce0fb6de31b42a24768f274ecb4c1e2ed8529e073cfb361fc1ef5d1e2d75a31"),
			Identity: &identityprovider.Identity{
				ID:        "03e0480538c2a39951d054e17ff31fde487cb1031d0044a037b53ad2e028a3e77c",
				PublicKey: MustBytesFromHex(t, "048bef2231e64d5c7147bd4b8afb84abd4126ee8d8335e4b069ac0a65c7be711cea5c1b8d47bc20ebaecdca588600ddf2894675e78b2ef17cf49e7bbaf98080361"),
				Signatures: &identityprovider.IdentitySignature{
					ID:        MustBytesFromHex(t, "3045022100f5f6f10571d14347aaf34e526ce3419fd64d75ffa7aa73692cbb6aeb6fbc147102203a3e3fa41fa8fcbb9fc7c148af5b640e2f704b20b3a4e0b93fc3a6d44dffb41e"),
					PublicKey: MustBytesFromHex(t, "3044022020982b8492be0c184dc29de0a3a3bd86a86ba997756b0bf41ddabd24b47c5acf02203745fda39d7df650a5a478e52bbe879f0cb45c074025a93471414a56077640a4"),
				},
				Type:     "orbitdb",
				Provider: id.Provider,
			},
			Hash:  MustCID(t, "zdpuAq7PAbQ7iavSdkNUUUrRUba5wSpRDJRsiC8RcvkXdgqYJ"),
			Clock: entry.NewLamportClock(MustBytesFromHex(t, "048bef2231e64d5c7147bd4b8afb84abd4126ee8d8335e4b069ac0a65c7be711cea5c1b8d47bc20ebaecdca588600ddf2894675e78b2ef17cf49e7bbaf98080361"), 3),
		},

		{
			Payload: []byte("four"),
			LogID:   "A",
			Next:    []cid.Cid{MustCID(t, "zdpuAq7PAbQ7iavSdkNUUUrRUba5wSpRDJRsiC8RcvkXdgqYJ")},
			V:       1,
			Key:     MustBytesFromHex(t, "048bef2231e64d5c7147bd4b8afb84abd4126ee8d8335e4b069ac0a65c7be711cea5c1b8d47bc20ebaecdca588600ddf2894675e78b2ef17cf49e7bbaf98080361"),
			Sig:     MustBytesFromHex(t, "30440220103ff89892856ec222d37b1244199cfb6e39629f155cd80ffa9b6e0b67de98940220391da8dc35e0b99f247c41676b8fb2337879d05dd343c55d9a89275c05076dcc"),
			Identity: &identityprovider.Identity{
				ID:        "03e0480538c2a39951d054e17ff31fde487cb1031d0044a037b53ad2e028a3e77c",
				PublicKey: MustBytesFromHex(t, "048bef2231e64d5c7147bd4b8afb84abd4126ee8d8335e4b069ac0a65c7be711cea5c1b8d47bc20ebaecdca588600ddf2894675e78b2ef17cf49e7bbaf98080361"),
				Signatures: &identityprovider.IdentitySignature{
					ID:        MustBytesFromHex(t, "3045022100f5f6f10571d14347aaf34e526ce3419fd64d75ffa7aa73692cbb6aeb6fbc147102203a3e3fa41fa8fcbb9fc7c148af5b640e2f704b20b3a4e0b93fc3a6d44dffb41e"),
					PublicKey: MustBytesFromHex(t, "3044022020982b8492be0c184dc29de0a3a3bd86a86ba997756b0bf41ddabd24b47c5acf02203745fda39d7df650a5a478e52bbe879f0cb45c074025a93471414a56077640a4"),
				},
				Type:     "orbitdb",
				Provider: id.Provider,
			},
			Hash:  MustCID(t, "zdpuAqgCh78NCXffmFYv4DM2KfhhpY92agJ9sKRB2eq9B5mFA"),
			Clock: entry.NewLamportClock(MustBytesFromHex(t, "048bef2231e64d5c7147bd4b8afb84abd4126ee8d8335e4b069ac0a65c7be711cea5c1b8d47bc20ebaecdca588600ddf2894675e78b2ef17cf49e7bbaf98080361"), 4),
		},

		{
			Payload: []byte("five"),
			LogID:   "A",
			Next:    []cid.Cid{MustCID(t, "zdpuAq7PAbQ7iavSdkNUUUrRUba5wSpRDJRsiC8RcvkXdgqYJ")},
			V:       1,
			Key:     MustBytesFromHex(t, "048bef2231e64d5c7147bd4b8afb84abd4126ee8d8335e4b069ac0a65c7be711cea5c1b8d47bc20ebaecdca588600ddf2894675e78b2ef17cf49e7bbaf98080361"),
			Sig:     MustBytesFromHex(t, "3044022012a6bad4be1aabec23816bc8ccaf3cb41d43f06adb3f7d55b14fe2ddae37035a02204324d0b9481c351a1b6c391bd9cb960c039f102f950cf2a48fd8648f7615c51f"),
			Identity: &identityprovider.Identity{
				ID:        "03e0480538c2a39951d054e17ff31fde487cb1031d0044a037b53ad2e028a3e77c",
				PublicKey: MustBytesFromHex(t, "048bef2231e64d5c7147bd4b8afb84abd4126ee8d8335e4b069ac0a65c7be711cea5c1b8d47bc20ebaecdca588600ddf2894675e78b2ef17cf49e7bbaf98080361"),
				Signatures: &identityprovider.IdentitySignature{
					ID:        MustBytesFromHex(t, "3045022100f5f6f10571d14347aaf34e526ce3419fd64d75ffa7aa73692cbb6aeb6fbc147102203a3e3fa41fa8fcbb9fc7c148af5b640e2f704b20b3a4e0b93fc3a6d44dffb41e"),
					PublicKey: MustBytesFromHex(t, "3044022020982b8492be0c184dc29de0a3a3bd86a86ba997756b0bf41ddabd24b47c5acf02203745fda39d7df650a5a478e52bbe879f0cb45c074025a93471414a56077640a4"),
				},
				Type:     "orbitdb",
				Provider: id.Provider,
			},
			Hash:  MustCID(t, "zdpuAwNuRc2Kc1aNDdcdSWuxfNpHRJQw8L8APBNHCEFuyU4Xf"),
			Clock: entry.NewLamportClock(MustBytesFromHex(t, "048bef2231e64d5c7147bd4b8afb84abd4126ee8d8335e4b069ac0a65c7be711cea5c1b8d47bc20ebaecdca588600ddf2894675e78b2ef17cf49e7bbaf98080361"), 4),
		},
	}
}
