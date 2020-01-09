package entry

import (
	"encoding/hex"
	"encoding/json"

	"berty.tech/go-ipfs-log/iface"
	"github.com/pkg/errors"
	"github.com/polydawn/refmt/obj/atlas"
)

type Hashable struct {
	Hash    interface{}
	ID      string
	Payload []byte
	Next    []string
	V       uint64
	Clock   iface.IPFSLogLamportClock
	Key     []byte
}

func init() {
	atlas.BuildEntry(Hashable{}).
		StructMap().
		AddField("Hash", atlas.StructMapEntry{SerialName: "hash"}).
		AddField("ID", atlas.StructMapEntry{SerialName: "id"}).
		AddField("Payload", atlas.StructMapEntry{SerialName: "payload"}).
		AddField("Next", atlas.StructMapEntry{SerialName: "next"}).
		AddField("V", atlas.StructMapEntry{SerialName: "v"}).
		AddField("Clock", atlas.StructMapEntry{SerialName: "clock"}).
		Complete()
}

// toBuffer converts a hashable entry to bytes.
func (e *Hashable) toBuffer() ([]byte, error) {
	if e == nil {
		return nil, errors.New("entry is not defined")
	}

	jsonBytes, err := json.Marshal(map[string]interface{}{
		"hash":    nil,
		"id":      e.ID,
		"payload": string(e.Payload),
		"next":    e.Next,
		"v":       e.V,
		"clock": map[string]interface{}{
			"id":   hex.EncodeToString(e.Clock.GetID()),
			"time": e.Clock.GetTime(),
		},
	})
	if err != nil {
		return nil, err
	}

	return jsonBytes, nil
}
