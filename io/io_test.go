package io_test
//
//import (
//	"fmt"
//	"github.com/berty/go-ipfs-log/entry"
//	"github.com/berty/go-ipfs-log/io"
//	"github.com/ipfs/go-cid"
//	"testing"
//)
//
//func TestWriteCBOR(t *testing.T) {
//	nextCID, err := cid.Parse("QmcFhL3tYpxayeovMcRGXf8RE9FdoX45hJm3ApCeRuZoGs")
//	if err != nil {
//		t.Errorf("unexpected error %+v", err)
//	}
//
//	nodeCid, err := io.WriteCBOR(io.NewMemoryServices(), &entry.Entry{
//		Next: nextCID,
//	})
//
//	if err != nil {
//		t.Errorf("unexpected error %+v", err)
//	}
//
//	fmt.Printf("CID: %+v\n\n\n", nodeCid.B58String())
//}
