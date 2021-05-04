package entry

import (
	"sync"

	"berty.tech/go-ipfs-log/iface"
	"github.com/iancoleman/orderedmap"
)

// OrderedMap is an ordered map of entries.
type OrderedMap struct {
	lock       sync.RWMutex
	orderedMap *orderedmap.OrderedMap
}

func (o *OrderedMap) Reverse() iface.IPFSLogOrderedEntries {
	e := o.Slice()

	for i := len(e)/2 - 1; i >= 0; i-- {
		opp := len(e) - 1 - i
		e[i], e[opp] = e[opp], e[i]
	}

	return NewOrderedMapFromEntries(e)
}

// NewOrderedMap creates a new OrderedMap of entries.
func NewOrderedMap() iface.IPFSLogOrderedEntries {
	return &OrderedMap{
		lock:       sync.RWMutex{},
		orderedMap: orderedmap.New(),
	}
}

// NewOrderedMapFromEntries creates a new OrderedMap of entries from a slice.
func NewOrderedMapFromEntries(entries []iface.IPFSLogEntry) iface.IPFSLogOrderedEntries {
	orderedMap := NewOrderedMap()

	for _, e := range entries {
		if e == nil || !e.Defined() {
			continue
		}

		orderedMap.Set(e.GetHash().String(), e)
	}

	return orderedMap
}

// Merge will fusion two OrderedMap of entries.
func (o *OrderedMap) Merge(other iface.IPFSLogOrderedEntries) iface.IPFSLogOrderedEntries {
	newMap := o.Copy()

	for _, k := range other.Keys() {
		val, _ := other.Get(k)
		newMap.Set(k, val)
	}

	return newMap
}

// Copy creates a copy of an OrderedMap.
func (o *OrderedMap) Copy() iface.IPFSLogOrderedEntries {
	o.lock.RLock()
	defer o.lock.RUnlock()

	newMap := NewOrderedMap()

	for _, k := range o.Keys() {
		val, _ := o.Get(k)
		newMap.Set(k, val)
	}

	return newMap
}

// Get retrieves an Entry using its key.
func (o *OrderedMap) Get(key string) (iface.IPFSLogEntry, bool) {
	o.lock.RLock()
	defer o.lock.RUnlock()

	val, exists := o.orderedMap.Get(key)
	if !exists {
		return nil, false
	}

	entry, ok := val.(iface.IPFSLogEntry)
	if !ok {
		return nil, false
	}

	return entry, true
}

// UnsafeGet retrieves an Entry using its key, returns nil if not found.
func (o *OrderedMap) UnsafeGet(key string) iface.IPFSLogEntry {
	o.lock.RLock()
	defer o.lock.RUnlock()

	val, _ := o.Get(key)

	return val
}

// Set defines an Entry in the map for a given key.
func (o *OrderedMap) Set(key string, value iface.IPFSLogEntry) {
	o.lock.Lock()
	defer o.lock.Unlock()

	o.orderedMap.Set(key, value)
}

// Slice returns an ordered slice of the values existing in the map.
func (o *OrderedMap) Slice() []iface.IPFSLogEntry {
	o.lock.RLock()
	defer o.lock.RUnlock()

	keys := o.orderedMap.Keys()
	out := make([]iface.IPFSLogEntry, len(keys))

	for i, k := range keys {
		out[i] = o.UnsafeGet(k)
	}

	return out
}

func (o *OrderedMap) First(until uint) iface.IPFSLogOrderedEntries {
	o.lock.RLock()
	defer o.lock.RUnlock()

	keys := o.Keys()
	entries := make([]iface.IPFSLogEntry, until)

	for i := uint(0); i < until; i++ {
		entries[i] = o.UnsafeGet(keys[i])
	}

	return NewOrderedMapFromEntries(entries)
}

func (o *OrderedMap) Last(after uint) iface.IPFSLogOrderedEntries {
	o.lock.RLock()
	defer o.lock.RUnlock()

	keys := o.Keys()
	entries := make([]iface.IPFSLogEntry, uint(len(keys))-after)
	j := 0

	for i := uint(len(keys)); i > after; i-- {
		entries[j] = o.UnsafeGet(keys[i])
		j++
	}

	return NewOrderedMapFromEntries(entries)
}

// Delete removes an Entry from the map for a given key.
func (o *OrderedMap) Delete(key string) {
	o.lock.Lock()
	defer o.lock.Unlock()

	o.orderedMap.Delete(key)
}

// Keys retrieves the ordered list of keys in the map.
func (o *OrderedMap) Keys() []string {
	o.lock.RLock()
	defer o.lock.RUnlock()

	return o.orderedMap.Keys()
}

// SortKeys orders the map keys using your sort func.
func (o *OrderedMap) SortKeys(sortFunc func(keys []string)) {
	o.lock.Lock()
	defer o.lock.Unlock()

	o.orderedMap.SortKeys(sortFunc)
}

// Sort orders the map using your sort func.
func (o *OrderedMap) Sort(lessFunc func(a *orderedmap.Pair, b *orderedmap.Pair) bool) {
	o.lock.Lock()
	defer o.lock.Unlock()

	o.orderedMap.Sort(lessFunc)
}

// Len gets the length of the map.
func (o *OrderedMap) Len() int {
	o.lock.RLock()
	defer o.lock.RUnlock()

	return len(o.orderedMap.Keys())
}

// At gets an item at the given index in the map, returns nil if not found.
func (o *OrderedMap) At(index uint) iface.IPFSLogEntry {
	o.lock.RLock()
	defer o.lock.RUnlock()

	keys := o.Keys()

	if keys == nil {
		return nil
	}

	if uint(len(keys)) <= index {
		return nil
	}

	key := keys[index]

	return o.UnsafeGet(key)
}

var _ iface.IPFSLogOrderedEntries = (*OrderedMap)(nil)
