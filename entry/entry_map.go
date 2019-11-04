package entry

import (
	"berty.tech/go-ipfs-log/iface"
	"github.com/iancoleman/orderedmap"
	"sync"
)

// OrderedMap is an ordered map of entries.
type OrderedMap struct {
	lock       sync.RWMutex
	orderedMap *orderedmap.OrderedMap
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
		if e == nil {
			continue
		}

		orderedMap.Set(e.GetHash().String(), e)
	}

	return orderedMap
}

// Merge will fusion two OrderedMap of entries.
func (o *OrderedMap) Merge(other iface.IPFSLogOrderedEntries) iface.IPFSLogOrderedEntries {
	newMap := o.Copy()

	otherKeys := other.Keys()
	for _, k := range otherKeys {
		val, _ := other.Get(k)
		newMap.Set(k, val)
	}

	return newMap
}

// Copy creates a copy of an OrderedMap.
func (o *OrderedMap) Copy() iface.IPFSLogOrderedEntries {
	newMap := NewOrderedMap()
	keys := o.Keys()

	for _, k := range keys {
		val, _ := o.Get(k)
		newMap.Set(k, val)
	}

	return newMap
}

// Get retrieves an Entry using its key.
func (o *OrderedMap) Get(key string) (iface.IPFSLogEntry, bool) {
	o.lock.RLock()
	val, exists := o.orderedMap.Get(key)
	entry, ok := val.(iface.IPFSLogEntry)
	if !ok {
		exists = false
	}
	o.lock.RUnlock()

	return entry, exists
}

// UnsafeGet retrieves an Entry using its key, returns nil if not found.
func (o *OrderedMap) UnsafeGet(key string) iface.IPFSLogEntry {
	o.lock.RLock()
	val, _ := o.Get(key)
	o.lock.RUnlock()

	return val
}

// Set defines an Entry in the map for a given key.
func (o *OrderedMap) Set(key string, value iface.IPFSLogEntry) {
	o.lock.Lock()
	o.orderedMap.Set(key, value)
	o.lock.Unlock()
}

// Slice returns an ordered slice of the values existing in the map.
func (o *OrderedMap) Slice() []iface.IPFSLogEntry {
	var out []iface.IPFSLogEntry

	keys := o.orderedMap.Keys()
	for _, k := range keys {
		out = append(out, o.UnsafeGet(k))
	}

	return out
}

// Delete removes an Entry from the map for a given key.
func (o *OrderedMap) Delete(key string) {
	o.lock.Lock()
	o.orderedMap.Delete(key)
	o.lock.Unlock()
}

// Keys retrieves the ordered list of keys in the map.
func (o *OrderedMap) Keys() []string {
	o.lock.RLock()
	keys := o.orderedMap.Keys()
	o.lock.RUnlock()

	return keys
}

// SortKeys orders the map keys using your sort func.
func (o *OrderedMap) SortKeys(sortFunc func(keys []string)) {
	o.lock.Lock()
	o.orderedMap.SortKeys(sortFunc)
	o.lock.Unlock()
}

// Sort orders the map using your sort func.
func (o *OrderedMap) Sort(lessFunc func(a *orderedmap.Pair, b *orderedmap.Pair) bool) {
	o.lock.Lock()
	o.orderedMap.Sort(lessFunc)
	o.lock.Unlock()
}

// Len gets the length of the map.
func (o *OrderedMap) Len() int {
	return len(o.orderedMap.Keys())
}

// At gets an item at the given index in the map, returns nil if not found.
func (o *OrderedMap) At(index uint) iface.IPFSLogEntry {
	keys := o.Keys()

	if uint(len(keys)) < index {
		return nil
	}

	return o.UnsafeGet(keys[index])
}

var _ iface.IPFSLogOrderedEntries = (*OrderedMap)(nil)
