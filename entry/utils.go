package entry // import "berty.tech/go-ipfs-log/entry"

import (
	"bytes"
	"sort"

	"github.com/iancoleman/orderedmap"
)

// Difference gets the list of values not present in both entries sets.
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

//func FindTails(entries []*Entry) []*Entry {
//	// Reverse index { next -> entry }
//	reverseIndex := map[string][]*Entry{}
//	// Null index containing entries that have no parents (nexts)
//	nullIndex := []*Entry{}
//	// Hashes for all entries for quick lookups
//	hashes := map[string]bool{}
//	// Hashes of all next entries
//	nexts := []cid.Cid{}
//
//	for _, e := range entries {
//		if len(e.Next) == 0 {
//			nullIndex = append(nullIndex, e)
//		}
//
//		for _, nextE := range e.Next {
//			reverseIndex[nextE.String()] = append(reverseIndex[nextE.String()], e)
//		}
//
//		nexts = append(nexts, e.Next...)
//
//		hashes[e.Hash.String()] = true
//	}
//
//	tails := []*Entry{}
//
//	for _, n := range nexts {
//		if _, ok := hashes[n.String()]; !ok {
//			continue
//		}
//
//		tails = append(tails, reverseIndex[n.String()]...)
//	}
//
//	tails = append(tails, nullIndex...)
//
//	return NewOrderedMapFromEntries(tails).Slice()
//}
//
//func FindTailHashes(entries []*Entry) []string {
//	res := []string{}
//	hashes := map[string]bool{}
//	for _, e := range entries {
//		hashes[e.Hash.String()] = true
//	}
//
//	for _, e := range entries {
//		nextLength := len(e.Next)
//
//		for i := range e.Next {
//			next := e.Next[nextLength-i]
//			if _, ok := hashes[next.String()]; !ok {
//				res = append([]string{e.Hash.String()}, res...)
//			}
//		}
//	}
//
//	return res
//}

// FindHeads search entries heads in an OrderedMap.
func FindHeads(entries *OrderedMap) []*Entry {
	if entries == nil {
		return nil
	}

	result := []*Entry{}
	items := orderedmap.New()

	for _, k := range entries.Keys() {
		e := entries.UnsafeGet(k)
		for _, n := range e.Next {
			items.Set(n.String(), e.Hash.String())
		}
	}

	for _, h := range entries.Keys() {
		e, ok := items.Get(h)
		if ok || e != nil {
			continue
		}

		result = append(result, entries.UnsafeGet(h))
	}

	sort.SliceStable(result, func(a, b int) bool {
		return bytes.Compare(result[a].Clock.ID, result[b].Clock.ID) < 0
	})

	return result
}
