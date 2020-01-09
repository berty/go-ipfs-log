package test // import "berty.tech/go-ipfs-log/test"

import (
	"berty.tech/go-ipfs-log/iface"
)

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
