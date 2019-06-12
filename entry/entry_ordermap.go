package entry // import "berty.tech/go-ipfs-log/entry"

import (
	"github.com/iancoleman/orderedmap"
)

type OrderedMap struct {
	orderedMap *orderedmap.OrderedMap
}

func NewOrderedMap() *OrderedMap {
	return &OrderedMap{
		orderedMap: orderedmap.New(),
	}
}

func NewOrderedMapFromEntries(entries []*Entry) *OrderedMap {
	orderedMap := NewOrderedMap()

	for _, e := range entries {
		if e == nil {
			continue
		}

		orderedMap.Set(e.Hash.String(), e)
	}

	return orderedMap
}

func (o *OrderedMap) Merge(other *OrderedMap) *OrderedMap {
	newMap := o.Copy()

	otherKeys := other.Keys()
	for _, k := range otherKeys {
		val, _ := other.Get(k)
		newMap.Set(k, val)
	}

	return newMap
}

func (o *OrderedMap) Copy() *OrderedMap {
	newMap := NewOrderedMap()
	keys := o.Keys()

	for _, k := range keys {
		val, _ := o.Get(k)
		newMap.Set(k, val)
	}

	return newMap
}

func (o *OrderedMap) Get(key string) (*Entry, bool) {
	val, exists := o.orderedMap.Get(key)
	entry, ok := val.(*Entry)
	if !ok {
		exists = false
	}

	return entry, exists
}

func (o *OrderedMap) UnsafeGet(key string) *Entry {
	val, _ := o.Get(key)

	return val
}

func (o *OrderedMap) Set(key string, value *Entry) {
	o.orderedMap.Set(key, value)
}

func (o *OrderedMap) Slice() []*Entry {
	out := []*Entry{}

	keys := o.orderedMap.Keys()
	for _, k := range keys {
		out = append(out, o.UnsafeGet(k))
	}

	return out
}

func (o *OrderedMap) Delete(key string) {
	o.orderedMap.Delete(key)
}

func (o *OrderedMap) Keys() []string {
	return o.orderedMap.Keys()
}

// SortKeys Sort the map keys using your sort func
func (o *OrderedMap) SortKeys(sortFunc func(keys []string)) {
	o.orderedMap.SortKeys(sortFunc)
}

// Sort Sort the map using your sort func
func (o *OrderedMap) Sort(lessFunc func(a *orderedmap.Pair, b *orderedmap.Pair) bool) {
	o.orderedMap.Sort(lessFunc)
}

func (o *OrderedMap) Len() int {
	return len(o.orderedMap.Keys())
}

func (o *OrderedMap) At(index uint) *Entry {
	keys := o.Keys()

	if uint(len(keys)) < index {
		return nil
	}

	return o.UnsafeGet(keys[index])
}
