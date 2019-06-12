package test // import "berty.tech/go-ipfs-log/test"

import "berty.tech/go-ipfs-log/entry"

func lastEntry(entries []*entry.Entry) *entry.Entry {
	length := len(entries)
	if length > 0 {
		return entries[len(entries)-1]
	}

	return nil
}

func entriesAsStrings(values *entry.OrderedMap) []string {
	var foundEntries []string
	for _, k := range values.Keys() {
		foundEntries = append(foundEntries, string(values.UnsafeGet(k).Payload))
	}

	return foundEntries
}

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
