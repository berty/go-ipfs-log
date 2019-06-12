package lamportclock // import "berty.tech/go-ipfs-log/utils/lamportclock"

import (
	"bytes"
	"encoding/hex"
	"math"

	cbornode "github.com/ipfs/go-ipld-cbor"
	"github.com/polydawn/refmt/obj/atlas"
)

type LamportClock struct {
	ID   []byte
	Time int
}

func (l *LamportClock) Tick() *LamportClock {
	l.Time++

	return &LamportClock{
		ID:   l.ID,
		Time: l.Time,
	}
}

func (l *LamportClock) Merge(clock *LamportClock) *LamportClock {
	l.Time = int(math.Max(float64(l.Time), float64(clock.Time)))

	return &LamportClock{
		ID:   l.ID,
		Time: l.Time,
	}
}

func (l *LamportClock) Clone() *LamportClock {
	return &LamportClock{
		ID:   l.ID,
		Time: l.Time,
	}
}

// Compare Calculate the "distance" based on the clock, ie. lower or greater
func Compare(a *LamportClock, b *LamportClock) int {
	// TODO: Make it a Golang slice-compatible sort function
	dist := a.Time - b.Time

	// If the sequence number is the same (concurrent events),
	// return the comparison between IDs
	if dist == 0 {
		return bytes.Compare(a.ID, b.ID)
	}

	return dist
}

func New(identity []byte, time int) *LamportClock {
	return &LamportClock{
		ID:   identity,
		Time: time,
	}
}

type CborLamportClock struct {
	ID   string
	Time int
}

func (l *LamportClock) ToCborLamportClock() *CborLamportClock {
	return &CborLamportClock{
		ID:   hex.EncodeToString(l.ID),
		Time: l.Time,
	}
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

var AtlasLamportClock = atlas.BuildEntry(CborLamportClock{}).
	StructMap().
	AddField("ID", atlas.StructMapEntry{SerialName: "id"}).
	AddField("Time", atlas.StructMapEntry{SerialName: "time"}).
	Complete()

func init() {
	cbornode.RegisterCborType(AtlasLamportClock)
}
