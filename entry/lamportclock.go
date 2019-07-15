package entry // import "berty.tech/go-ipfs-log/entry"

import (
	"bytes"
	"encoding/hex"
	"math"

	"github.com/polydawn/refmt/obj/atlas"

	cbornode "github.com/ipfs/go-ipld-cbor"
)

type LamportClock struct {
	ID   []byte `json:"id,omitempty"`
	Time int    `json:"time,omitempty"`
}

// Tick increments the time value, returns a new instance of LamportClock.
func (l *LamportClock) Tick() *LamportClock {
	l.Time++

	return &LamportClock{
		ID:   l.ID,
		Time: l.Time,
	}
}

// Merge fusion two LamportClocks.
func (l *LamportClock) Merge(clock *LamportClock) *LamportClock {
	l.Time = int(math.Max(float64(l.Time), float64(clock.Time)))

	return &LamportClock{
		ID:   l.ID,
		Time: l.Time,
	}
}

// Compare calculate the "distance" based on the clock, ie. lower or greater.
func (l *LamportClock) Compare(b *LamportClock) int {
	// TODO: Make it a Golang slice-compatible sort function
	dist := l.Time - b.Time

	// If the sequence number is the same (concurrent events),
	// return the comparison between IDs
	if dist == 0 {
		return bytes.Compare(l.ID, b.ID)
	}

	return dist
}

// NewLamportClock creates a new LamportClock instance.
func NewLamportClock(identity []byte, time int) *LamportClock {
	return &LamportClock{
		ID:   identity,
		Time: time,
	}
}

type cborLamportClock struct {
	ID   string
	Time int
}

func (l *LamportClock) toCborLamportClock() *cborLamportClock {
	return &cborLamportClock{
		ID:   hex.EncodeToString(l.ID),
		Time: l.Time,
	}
}

func (c *cborLamportClock) toLamportClock() (*LamportClock, error) {
	id, err := hex.DecodeString(c.ID)
	if err != nil {
		return nil, err
	}

	return &LamportClock{
		ID:   id,
		Time: c.Time,
	}, nil
}

func init() {
	var AtlasLamportClock = atlas.BuildEntry(cborLamportClock{}).
		StructMap().
		AddField("ID", atlas.StructMapEntry{SerialName: "id"}).
		AddField("Time", atlas.StructMapEntry{SerialName: "time"}).
		Complete()

	cbornode.RegisterCborType(AtlasLamportClock)
}
