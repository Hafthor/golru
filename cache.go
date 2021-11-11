package golru

import (
	"fmt"
	"sync"
	"time"
)

const cacheEntryOverhead = 100

type CacheEntry struct {
	key, value interface{}
	err        error
	size       int64 // represents total cache size in root entry - does NOT include cacheEntryOverhead
	expiry     time.Time
	mutex      sync.RWMutex // entry.mutex only used while filling, root.mutex is used to lock the cache itself
	next, prev *CacheEntry  // doubly linked list to track lru
}

type Cache struct {
	// invariant
	nower   func() time.Time
	root    *CacheEntry // root.next=head, root.prev=tail, root.mutex=cache lock, root.size=cache size
	entries map[interface{}]*CacheEntry
	// variant
	maxCount                              int
	maxSize                               int64
	hit, miss, expired, evicted, replaced int64 // stats
	onEvicted                             func(key, value interface{}, err error)
}

func NewCache(maxCount int, maxSize int64) *Cache {
	root := &CacheEntry{key: "**root**"}
	root.prev = root
	root.next = root
	return &Cache{entries: make(map[interface{}]*CacheEntry), nower: time.Now, maxCount: maxCount, maxSize: maxSize, root: root}
}

func (c *Cache) SetMax(maxCount int, maxSize int64) {
	c.root.mutex.Lock()
	c.maxCount = maxCount
	c.maxSize = maxSize
	c.root.mutex.Unlock()
	c.adjustSize(false, false)
}

func (c *Cache) SetOnEvicted(fn func(key, value interface{}, err error)) {
	c.root.mutex.Lock()
	defer c.root.mutex.Unlock()
	c.onEvicted = fn
}

func (c *Cache) SweepExpired() int64 {
	return c.adjustSize(true, false)
}

func (c *Cache) Purge() {
	c.adjustSize(true, true) // runs evictions
}

func (c *Cache) Size() int64 {
	c.root.mutex.RLock()
	defer c.root.mutex.RUnlock()
	return c.root.size
}

func (c *Cache) Len() int {
	c.root.mutex.RLock()
	defer c.root.mutex.RUnlock()
	return len(c.entries)
}

// Stats returns:
//  * hits - number of times cache was used to Get value
//  * miss - number of times value was missing or expired out
//  * expired - number of times a cache entry was expired out for time
//  * evicted - number of times a cache entry was evicted for space
//  * replaced - number of times a cache entry was replaced (Put)
func (c *Cache) Stats() (hit, miss, expired, evicted, replaced int64) {
	c.root.mutex.RLock()
	defer c.root.mutex.RUnlock()
	return c.hit, c.miss, c.expired, c.evicted, c.replaced
}

// ResetStats
// returns stats while resetting their values to 0
func (c *Cache) ResetStats() (hit, miss, expired, evicted, replaced int64) {
	c.root.mutex.Lock()
	defer c.root.mutex.Unlock()
	hit, miss, expired, evicted, replaced = c.hit, c.miss, c.expired, c.evicted, c.replaced
	c.hit, c.miss, c.expired, c.evicted, c.replaced = 0, 0, 0, 0, 0
	return hit, miss, expired, evicted, replaced
}

// knit
// connects entry in linked list to head
// MUST be run under cache lock
// returns true if knitting took place
func (c *Cache) knit(entry *CacheEntry) bool {
	root := c.root
	if entry.next != nil || entry.prev != nil { return false }
	next := root.next
	entry.prev = root
	entry.next = next
	next.prev = entry
	root.next = entry
	return true
}

// snip
// removes entry from linked list
// MUST be run under cache lock
// returns true if snipping took place
func (c *Cache) snip(entry *CacheEntry) bool {
	if entry == c.root {
		panic("snip on root")
	}
	prev, next := entry.prev, entry.next
	// can happen when entry evicted before fill and another caller was waiting on it and tries to move it to head
	if prev == nil && next == nil {
		return false
	}
	prev.next = next
	entry.prev = nil
	entry.next = nil
	next.prev = prev
	return true
}

// evict
// removes entry from cache
// MUST be run under cache lock
// does NOT run onEvicted func
func (c *Cache) evict(entry *CacheEntry) bool {
	entry.mutex.RLock()
	snipped := c.snip(entry)
	if snipped {
		c.root.size -= entry.size + cacheEntryOverhead
		delete(c.entries, entry.key)
	}
	entry.mutex.RUnlock()
	return snipped
}

func (c *Cache) Evict(key interface{}) bool {
	root := c.root
	root.mutex.Lock()
	entry, ok := c.entries[key]
	if ok {
		c.evict(entry)
		c.evicted++
	}
	fnOnEvicted := c.onEvicted
	root.mutex.Unlock()
	if fnOnEvicted != nil && entry != nil {
		fnOnEvicted(entry.key, entry.value, entry.err)
	}
	return ok
}

func (c *Cache) Get(key interface{}, fnFiller func() (interface{}, time.Duration, int64, error)) (interface{}, error) {
	now := c.nower()
	root := c.root
	root.mutex.Lock()
	fnOnEvicted := c.onEvicted
	var evicted *CacheEntry
	if entry, ok := c.entries[key]; ok {
		root.mutex.Unlock()
		entry.mutex.RLock() // wait until entry filled
		expiry := entry.expiry
		size := entry.size
		entry.mutex.RUnlock()
		root.mutex.Lock()
		entry2, ok := c.entries[key]
		if !ok {
			// near-death recovery
			if c.knit(entry) {
				root.size += size + cacheEntryOverhead
			}
			c.entries[key] = entry
			val, err := entry.value, entry.err
			c.hit++
			root.mutex.Unlock()
			c.adjustSize(false, false)
			return val, err
		}
		if entry != entry2 {
			// brand-new value, probably being filled
			entry = entry2
			root.mutex.Unlock()
			entry.mutex.RLock() // wait until entry filled
			expiry = entry.expiry
			size = entry.size
			entry.mutex.RUnlock()
			root.mutex.Lock()
			// this all could potentially happen again, but we are going to continue, risking a minor accounting error
		}
		if expiry.After(now) {
			//fmt.Print(".")
			// move to head
			if c.snip(entry) {
				root.size -= size + cacheEntryOverhead
			}
			if c.knit(entry) {
				root.size += size + cacheEntryOverhead
			}
			val, err := entry.value, entry.err
			c.hit++
			root.mutex.Unlock()
			return val, err
		} else {
			//fmt.Print("^")
			c.evict(entry)
			c.expired++
			evicted = entry
		}
	}

	if fnFiller == nil {
		c.miss++
		root.mutex.Unlock()
		return nil, nil
	}

	//fmt.Print("*")
	entry := &CacheEntry{key: key}

	entry.mutex.Lock()
	c.entries[key] = entry
	root.size += cacheEntryOverhead
	c.knit(entry)
	c.miss++
	root.mutex.Unlock()

	val, ttl, size, err := fnFiller()
	now = c.nower() // now after we run the fetcher function
	expiry := now.Add(ttl)

	entry.value = val
	entry.expiry = expiry
	entry.size = size
	entry.err = err
	entry.mutex.Unlock()

	// this entry could possibly get evicted before adjust size under lock but
	// the size would just be over adjusted down, and this will just make it right
	// this does mean size could potentially end up negative here. don't freak out.

	root.mutex.Lock()
	root.size += size
	root.mutex.Unlock()

	if evicted != nil && fnOnEvicted != nil {
		fnOnEvicted(evicted.key, evicted.value, entry.err)
	}

	c.adjustSize(false, false)
	return val, err
}

func (c *Cache) Gets(keys []interface{}, fnFiller func([]interface{}) ([]interface{}, time.Duration, []int64, error)) ([]interface{}, error) {
	now := c.nower()
	root := c.root
	entries := make([]*CacheEntry, len(keys))
	returnValues := make([]interface{}, len(keys))
	var fetchKeys []interface{}
	var fetchKeyIndex []int
	var evicted []*CacheEntry
	root.mutex.Lock()
	fnOnEvicted := c.onEvicted
	for i, key := range keys {
		entry, ok := c.entries[key]
		if ok {
			entry.mutex.RLock() // wait until entry filled
			entry.mutex.RUnlock()
			if entry.expiry.After(now) {
				c.hit++
				entries[i] = entry
				// move to head
				c.snip(entry)
				c.knit(entry)
				returnValues[i] = entry.value
			} else {
				c.evict(entry)
				c.miss++
				c.expired++
				evicted = append(evicted, entry)
				entry = nil
			}
		} else {
			c.miss++
		}

		if entry == nil && fnFiller != nil {
			entry = &CacheEntry{key: key}
			entries[i] = entry
			fetchKeys = append(fetchKeys, key)
			fetchKeyIndex = append(fetchKeyIndex, i)
			entry.mutex.Lock()
			c.entries[key] = entry
			c.knit(entry)
		}
	}

	root.mutex.Unlock()
	if fnFiller == nil {
		return returnValues, nil
	}

	var fetchedValues []interface{}
	var ttl time.Duration
	var sizes []int64
	var err error
	if len(fetchKeys) > 0 {
		fetchedValues, ttl, sizes, err = fnFiller(fetchKeys)
	}
	// now after we run the fetcher function
	expiry := c.nower().Add(ttl)

	size := int64(0)
	for i, entryIndex := range fetchKeyIndex[:] {
		val := fetchedValues[i]
		entry := entries[entryIndex]
		entry.value = val
		returnValues[entryIndex] = val
		entry.expiry = expiry
		entry.err = err
		entry.size = sizes[i]
		entry.mutex.Unlock()
		size += sizes[i] + cacheEntryOverhead
	}

	if fnOnEvicted != nil && len(evicted) > 0 {
		for _, entry := range evicted[:] {
			fnOnEvicted(entry.key, entry.value, entry.err)
		}
	}
	// this entry could possibly get evicted before adjust size under lock but
	// the size would just be over adjusted down, and this will just make it right
	// this does mean size could potentially end up negative here. don't freak out.

	root.mutex.Lock()
	root.size += size
	root.mutex.Unlock()

	c.adjustSize(false, false)
	return returnValues, err
}

func (c *Cache) Put(key, value interface{}, duration time.Duration, size int64, err error) {
	e := &CacheEntry{
		key:    key,
		value:  value,
		err:    err,
		size:   size,
		expiry: c.nower().Add(duration),
	}
	root := c.root
	root.mutex.Lock()
	fnOnEvicted := c.onEvicted
	oldEntry, hasOld := c.entries[key]
	if hasOld {
		c.evict(oldEntry)
		c.replaced++
	}
	if c.knit(e) {
		root.size += size + cacheEntryOverhead
	}
	c.entries[key] = e
	root.mutex.Unlock()

	if fnOnEvicted != nil && hasOld {
		fnOnEvicted(oldEntry.key, oldEntry.value, oldEntry.err)
	}
}

// sweep cache for expired entries or will evict
// least recently used entries if more space needed
// MUST be called under cache lock
func (c *Cache) adjustSize(expire, purge bool) int64 {
	now := c.nower()
	root := c.root
	root.mutex.Lock()
	maxCountExpire, maxSizeExpire := c.maxCount, c.maxSize
	maxCountEvict, maxSizeEvict := c.maxCount, c.maxSize
	if expire || purge {
		maxCountExpire, maxSizeExpire = 0, 0
		if purge {
			maxCountEvict, maxSizeEvict = 0, 0
		}
	}
	fnOnEvicted := c.onEvicted
	var evicted []*CacheEntry

	if root.size > maxSizeExpire || len(c.entries) > maxCountExpire {
		// evict stale entries
		entry := root
		var size int64
		for true {
			e := entry.next
			if e == root {
				break
			} else {
				e.mutex.RLock()
				expiry := e.expiry
				sz := e.size
				e.mutex.RUnlock()
				if expiry.Before(now) {
					c.evict(e)
					c.expired++
					evicted = append(evicted, e)
				} else {
					entry = e
					size += sz + cacheEntryOverhead
				}
			}
		}
		if root.size != size {
			fmt.Printf("WARNING: cache accounting fail - was %d, but actual is %d\n", root.size, size)
			root.size = size
		}

		root.mutex.Unlock()
		root.mutex.Lock()

		// if we are still over, drop entries off tail
		for true {
			entry := root.prev
			if entry == root { break }
			if root.size <= maxSizeEvict && len(c.entries) <= maxCountEvict { break }
			if c.evict(entry) {
				evicted = append(evicted, entry)
				c.evicted++
			}
		}
	}
	newSize := root.size
	root.mutex.Unlock()

	if fnOnEvicted != nil && len(evicted) > 0 {
		for _, entry := range evicted[:] {
			fnOnEvicted(entry.key, entry.value, entry.err)
		}
	}
	return newSize
}
