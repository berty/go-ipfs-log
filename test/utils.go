package test

import "github.com/berty/go-ipfs-log/entry"

func getLastEntry(omap *entry.OrderedMap) *entry.Entry {
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
