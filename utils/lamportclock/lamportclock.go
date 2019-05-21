package lamportclock

import (
	"bytes"
	cbornode "github.com/ipfs/go-ipld-cbor"
	ic "github.com/libp2p/go-libp2p-crypto"
	"github.com/polydawn/refmt/obj/atlas"
	"math"
)

type LamportClock struct {
	ID   *ic.Secp256k1PublicKey
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
	if dist == 0 && a.ID != b.ID {
		aBytes, err := a.ID.Bytes()
		if err != nil {
			return 0, err
		}

		bBytes, err := b.ID.Bytes()
		if err != nil {
			panic(err)
		}

		if bytes.Compare(aBytes, bBytes) < 0 {
			return -1, nil
		}

		return 1, nil
	}

	return int(dist), nil
}

func New(identity *ic.Secp256k1PublicKey, time int) *LamportClock {
	return &LamportClock{
		ID:   identity,
		Time: time,
	}
}

var AtlasLamportClock = atlas.BuildEntry(LamportClock{}).
	StructMap().
	AddField("ID", atlas.StructMapEntry{SerialName: "id"}).
	AddField("Time", atlas.StructMapEntry{SerialName: "time"}).
	Complete()


func init() {
	cbornode.RegisterCborType(AtlasLamportClock)
}
