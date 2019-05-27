package lamportclock

import (
	"bytes"
	"encoding/hex"
	cbornode "github.com/ipfs/go-ipld-cbor"
	"github.com/polydawn/refmt/obj/atlas"
	"math"
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
func Compare(a *LamportClock, b *LamportClock) (int, error) {
	// TODO: Make it a Golang slice-compatible sort function

	var dist = a.Time - b.Time

	// If the sequence number is the same (concurrent events),
	// and the IDs are different, take the one with a "lower" id
	if dist == 0 {
		comp := bytes.Compare(a.ID, b.ID)

		if comp < 0 {
			return -1, nil
		} else if comp > 0 {
			return 1, nil
		}
	}

	return int(dist), nil
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
		ID: hex.EncodeToString(l.ID),
		Time: l.Time,
	}
}

func (c *CborLamportClock) ToLamportClock() (*LamportClock, error) {
	id, err := hex.DecodeString(c.ID)
	if err != nil {
		return nil, err
	}

	return &LamportClock{
		ID: id,
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
