package ipfslog

import "berty.tech/go-ipfs-log/iface"

func maxInt(x, y int) int {
	if x < y {
		return y
	}
	return x
}

func maxClockTimeForEntries(entries []iface.IPFSLogEntry, defValue int) int {
	max := defValue
	for _, e := range entries {
		max = maxInt(e.GetClock().GetTime(), max)
	}

	return max
}
