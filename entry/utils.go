package entry

import (
	"sort"
)

func EntriesAsStrings(entries []*Entry) []string {
	var values []string

	for _, e := range entries {
		values = append(values, string(e.Payload))
	}

	sort.Strings(values)

	return values
}

func Slice(entries []*Entry, index int) []*Entry {
	if len(entries) == 0 || index >= len(entries) {
		return []*Entry{}
	}

	if index == 0 || (index < 0 && -index >= len(entries)) {
		return entries
	}

	if index > 0 {
		return entries[index:]
	}

	return entries[(len(entries) + index):]
}

func SliceRange(entries []*Entry, from int, to int) []*Entry {
	if len(entries) == 0 {
		return []*Entry{}
	}

	if from < 0 {
		from = len(entries) + from
		if from < 0 {
			from = 0
		}
	}

	if to < 0 {
		to = len(entries) + to
	}

	if from >= len(entries) {
		return []*Entry{}
	}

	if to > len(entries) {
		to = len(entries)
	}

	if from >= to {
		return []*Entry{}
	}

	if from == to {
		return entries
	}

	return entries[from:to]
}

func Difference(a []*Entry, b []*Entry) []*Entry {
	existing := map[string]bool{}
	processed := map[string]bool{}
	var diff []*Entry

	for _, v := range a {
		existing[v.Hash.String()] = true
	}

	for _, v := range b {
		isInFirst := existing[v.Hash.String()]
		hasBeenProcessed := processed[v.Hash.String()]
		if !isInFirst && !hasBeenProcessed {
			diff = append(diff, v)
			processed[v.Hash.String()] = true
		}
	}

	return diff
}
