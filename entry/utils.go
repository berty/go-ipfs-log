package entry

import "sort"

func EntriesAsStrings(entries []*Entry) []string {
	var values []string

	for _, e := range entries {
		values = append(values, string(e.Payload))
	}

	sort.Strings(values)

	return values
}
