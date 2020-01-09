package entry

import (
	"encoding/hex"

	cbornode "github.com/ipfs/go-ipld-cbor"
	"github.com/polydawn/refmt/obj/atlas"
)

func init() {
	var AtlasLamportClock = atlas.BuildEntry(CborLamportClock{}).
		StructMap().
		AddField("ID", atlas.StructMapEntry{SerialName: "id"}).
		AddField("Time", atlas.StructMapEntry{SerialName: "time"}).
		Complete()

	cbornode.RegisterCborType(AtlasLamportClock)
}

type CborLamportClock struct {
	ID   string
	Time int
}

func (c *CborLamportClock) ToLamportClock() (*LamportClock, error) {
	id, err := hex.DecodeString(c.ID)
	if err != nil {
		return nil, err
	}

	return &LamportClock{
		ID:   id,
		Time: c.Time,
	}, nil
}
