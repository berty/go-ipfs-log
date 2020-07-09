package test

import (
	"context"
	"testing"

	"berty.tech/go-ipfs-log/iface"
	ipfsCore "github.com/ipfs/go-ipfs/core"
	"github.com/ipfs/go-ipfs/core/coreapi"
	mock "github.com/ipfs/go-ipfs/core/mock"
	core_iface "github.com/ipfs/interface-go-ipfs-core"
	mocknet "github.com/libp2p/go-libp2p/p2p/net/mock"
	"github.com/stretchr/testify/require"
)

func NewMemoryServices(ctx context.Context, t testing.TB, m mocknet.Mocknet) (core_iface.CoreAPI, func()) {
	t.Helper()

	core, err := ipfsCore.NewNode(ctx, &ipfsCore.BuildCfg{
		Online: true,
		Host:   mock.MockHostOption(m),
		ExtraOpts: map[string]bool{
			"pubsub": true,
		},
	})
	require.NoError(t, err)

	api, err := coreapi.NewCoreAPI(core)
	require.NoError(t, err)

	close := func() {
		core.Close()
	}
	return api, close
}

func lastEntry(entries []iface.IPFSLogEntry) iface.IPFSLogEntry {
	length := len(entries)
	if length > 0 {
		return entries[len(entries)-1]
	}

	return nil
}

func entriesAsStrings(values iface.IPFSLogOrderedEntries) []string {
	var foundEntries []string
	for _, v := range values.Slice() {
		foundEntries = append(foundEntries, string(v.GetPayload()))
	}

	return foundEntries
}

func getLastEntry(omap iface.IPFSLogOrderedEntries) iface.IPFSLogEntry {
	lastKey := omap.Keys()[len(omap.Keys())-1]

	return omap.UnsafeGet(lastKey)
}

func minInt(a, b int) int {
	if a < b {
		return a
	}
	return b
}

func intPtr(val int) *int {
	return &val
}

var bigLogString = `DONE
└─EOF
  └─entryC10
    └─entryB10
      └─entryA10
    └─entryC9
      └─entryB9
        └─entryA9
      └─entryC8
        └─entryB8
          └─entryA8
        └─entryC7
          └─entryB7
            └─entryA7
          └─entryC6
            └─entryB6
              └─entryA6
            └─entryC5
              └─entryB5
                └─entryA5
              └─entryC4
                └─entryB4
                  └─entryA4
└─3
                └─entryC3
                  └─entryB3
                    └─entryA3
  └─2
                  └─entryC2
                    └─entryB2
                      └─entryA2
    └─1
                    └─entryC1
                      └─entryB1
                        └─entryA1`
