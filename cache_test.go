package golru

import (
	"fmt"
	"math/rand"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestCache(t *testing.T) {
	t.Run("should cache value", func(t *testing.T) {
		cache := NewCache(10, 200)

		v, e := cache.Get("test", func() (interface{}, time.Duration, int64, error) {
			return "test", time.Minute, 4 + 4, nil
		})
		if v != "test" || e != nil {
			t.Fatalf("unexpected values v=%v, e=%v", v, e)
		}
		s, l := cache.Size(), cache.Len()
		if s != cacheEntryOverhead+8 || l != 1 {
			t.Fatalf("unexpected values s=%d, l=%d", s, l)
		}
		cache.Put("test", "new", time.Minute, 7, nil)
		hit, miss, expired, evicted, replaced := cache.Stats()
		if hit != 0 || miss != 1 || expired != 0 || evicted != 0 || replaced != 1 {
			t.Fatalf("unexpected stats hit=%d, miss=%d, expired=%d, evicted=%d, replaced=%d", hit, miss, expired, evicted, replaced)
		}
		s, l = cache.Size(), cache.Len()
		if s != cacheEntryOverhead+7 || l != 1 {
			t.Fatalf("unexpected values s=%d, l=%d", s, l)
		}
	})

	t.Run("should evict cache value without onEvicted function", func(t *testing.T) {
		cache := NewCache(10, 200)

		v, e := cache.Get("test", func() (interface{}, time.Duration, int64, error) {
			return "test", time.Minute, 4 + 4, nil
		})
		if v != "test" || e != nil {
			t.Fatalf("unexpected values v=%v, e=%v", v, e)
		}
		cache.Evict("test")
		s, l := cache.Size(), cache.Len()
		if s != 0 || l != 0 {
			t.Fatalf("unexpected values s=%d, l=%d", s, l)
		}
		v, e = cache.Get("test", func() (interface{}, time.Duration, int64, error) {
			return "test2", time.Minute, 4 + 5, nil
		})
		if v != "test2" || e != nil {
			t.Fatalf("unexpected values v=%v, e=%v", v, e)
		}
		s, l = cache.Size(), cache.Len()
		if s != cacheEntryOverhead+9 || l != 1 {
			t.Fatalf("unexpected values s=%d, l=%d", s, l)
		}
		hit, miss, expired, evicted, replaced := cache.Stats()
		if hit != 0 || miss != 2 || expired != 0 || evicted != 1 || replaced != 0 {
			t.Fatalf("unexpected stats hit=%d, miss=%d, expired=%d, evicted=%d, replaced=%d", hit, miss, expired, evicted, replaced)
		}
	})

	t.Run("should run evict function", func(t *testing.T) {
		cache := NewCache(10, 200)
		cache.Get("test-key", func() (interface{}, time.Duration, int64, error) {
			return "test-value", time.Minute, 8 + 10, nil
		})
		ch := make(chan interface{}, 2)
		cache.SetOnEvicted(func(key, value interface{}, err error) { ch <- key; ch <- value })
		cache.Evict("test-key")
		k, v := <-ch, <-ch
		if k != "test-key" || v != "test-value" {
			t.Fatalf("unexpected values k=%v, v=%v", k, v)
		}
		s, l := cache.Size(), cache.Len()
		if s != 0 || l != 0 {
			t.Fatalf("unexpected values s=%d, l=%d", s, l)
		}
		hit, miss, expired, evicted, replaced := cache.Stats()
		if hit != 0 || miss != 1 || expired != 0 || evicted != 1 || replaced != 0 {
			t.Fatalf("unexpected stats hit=%d, miss=%d, expired=%d, evicted=%d, replaced=%d", hit, miss, expired, evicted, replaced)
		}
	})

	t.Run("should cache value and get new one", func(t *testing.T) {
		cache := NewCache(10, 500)
		ch := make(chan interface{}, 2)
		cache.SetOnEvicted(func(key, value interface{}, err error) { ch <- key; ch <- value })
		v, e := cache.Get("test", func() (interface{}, time.Duration, int64, error) {
			return "test", time.Minute, 4 + 4, nil
		})
		if v != "test" || e != nil {
			t.Fatalf("unexpected values v=%v, e=%v", v, e)
		}
		v, e = cache.Get("test", func() (interface{}, time.Duration, int64, error) {
			return "fail", time.Minute, 4 + 4, nil
		})
		if v != "test" || e != nil {
			t.Fatalf("unexpected values v=%v, e=%v", v, e)
		}

		cache.nower = func() time.Time { return time.Now().Add(time.Hour) }
		v, e = cache.Get("test", func() (interface{}, time.Duration, int64, error) {
			return "new", time.Minute, 4 + 3, nil
		})
		if v != "new" || e != nil {
			t.Fatalf("unexpected values v=%v, e=%v", v, e)
		}
		k, v := <-ch, <-ch
		if k != "test" || v != "test" {
			t.Fatalf("unexpected values k=%v, v=%v", k, v)
		}
	})

	t.Run("should evict stale entries", func(t *testing.T) {
		cache := NewCache(10, 300)
		ch := make(chan interface{}, 2)
		cache.SetOnEvicted(func(key, value interface{}, err error) { ch <- key; ch <- value })
		cache.Get("keep", func() (interface{}, time.Duration, int64, error) {
			return "keep", time.Minute, 4 + 4, nil
		})
		cache.Get("drop", func() (interface{}, time.Duration, int64, error) {
			return "drop", time.Second, 4 + 4, nil
		})
		cache.nower = func() time.Time { return time.Now().Add(time.Second * 2) }
		cache.Get("keep2", func() (interface{}, time.Duration, int64, error) {
			return "keep2", time.Minute, 5 + 5, nil
		})
		if v, e := cache.Get("keep", nil); v == nil || v != "keep" || e != nil {
			t.Fatalf("unexpected values v=%v, e=%v", v, e)
		}
		if v, e := cache.Get("keep2", nil); v == nil || v != "keep2" || e != nil {
			t.Fatalf("unexpected values v=%v, e=%v", v, e)
		}
		if v, e := cache.Get("drop", nil); v != nil || e != nil {
			t.Fatalf("unexpected values v=%v, e=%v", v, e)
		}
		k, v := <-ch, <-ch
		if k != "drop" || v != "drop" {
			t.Fatalf("unexpected values k=%v, v=%v", k, v)
		}
		s, l := cache.Size(), cache.Len()
		if s != cacheEntryOverhead*2+10+8 || l != 2 {
			t.Fatalf("unexpected values s=%d, l=%d", s, l)
		}
	})

	t.Run("should evict tail entries", func(t *testing.T) {
		cache := NewCache(10, 300)
		ch := make(chan interface{}, 2)
		cache.SetOnEvicted(func(key, value interface{}, err error) { ch <- key; ch <- value })
		cache.Get("keep", func() (interface{}, time.Duration, int64, error) {
			return "keep", time.Minute, 4 + 4, nil
		})
		cache.Get("drop", func() (interface{}, time.Duration, int64, error) {
			return "drop", time.Minute, 4 + 4, nil
		})
		cache.Get("keep", nil) // move to head
		cache.Get("keep2", func() (interface{}, time.Duration, int64, error) {
			return "keep2", time.Minute, 5 + 5, nil
		})
		if v, e := cache.Get("keep2", nil); v != "keep2" || e != nil {
			t.Fatalf("unexpected values v=%v, e=%v", v, e)
		}
		if v, e := cache.Get("keep", nil); v != "keep" || e != nil {
			t.Fatalf("unexpected values v=%v, e=%v", v, e)
		}
		if v, e := cache.Get("drop", nil); v != nil || e != nil {
			t.Fatalf("unexpected values v=%v, e=%v", v, e)
		}
		k, v := <-ch, <-ch
		if k != "drop" || v != "drop" {
			t.Fatalf("unexpected values k=%v, v=%v", k, v)
		}
		s, l := cache.Size(), cache.Len()
		if s != cacheEntryOverhead*2+8+10 || l != 2 {
			t.Fatalf("unexpected values s=%d, l=%d", s, l)
		}
	})

	t.Run("should stall subsequent Get calls", func(t *testing.T) {
		cache := NewCache(0, 0) // should work even when no space left
		ch1 := make(chan interface{})
		ch2 := make(chan interface{})
		ch3 := make(chan interface{})
		getStall1 := make(chan interface{})
		getStall2 := make(chan interface{})
		go func() {
			v, _ := cache.Get("keep", func() (interface{}, time.Duration, int64, error) {
				// wait to get value
				getStall1 <- nil
				getStall2 <- nil
				time.Sleep(time.Millisecond)
				return "keep", time.Minute, 4 + 4, nil
			})
			ch1 <- v // send back value fetched
		}()
		go func() {
			<-getStall1
			v, _ := cache.Get("keep", func() (interface{}, time.Duration, int64, error) {
				t.Errorf("should not have called second get")
				return "fail", time.Minute, 4 + 4, nil
			})
			ch2 <- v // send back value fetched
		}()
		go func() {
			<-getStall2
			v, _ := cache.Get("keep", nil) // should work even with no filler Get
			ch3 <- v                       // send back value fetched
		}()

		v1, v2, v3 := <-ch1, <-ch2, <-ch3
		if v1 != "keep" || v2 != "keep" || v3 != "keep" {
			t.Fatalf("unexpected values v1=%v, v2=%v, v3=%v", v1, v2, v3)
		}
		s, l := cache.Size(), cache.Len()
		if s != 0 || l != 0 {
			t.Fatalf("unexpected values s=%d, l=%d", s, l)
		}
	})

	t.Run("should allow gets", func(t *testing.T) {
		cache := NewCache(10, 1000)
		ch := make(chan interface{}, 2)
		cache.SetOnEvicted(func(key, value interface{}, err error) { ch <- key; ch <- value })
		cache.Get("b", func() (interface{}, time.Duration, int64, error) {
			return "bb", time.Minute, 1 + 2, nil // caching entry
		})
		cache.Get("d", func() (interface{}, time.Duration, int64, error) {
			return "dd", -time.Minute, 1 + 2, nil // expired entry
		})

		keys := []interface{}{"a", "b", "c", "d"}
		vs, e := cache.Gets(keys, func(keys []interface{}) ([]interface{}, time.Duration, []int64, error) {
			return keys, time.Minute, []int64{2, 2, 2, 2}, nil
		})
		if e != nil || len(vs) != 4 || vs[0] != "a" || vs[1] != "bb" || vs[2] != "c" || vs[3] != "d" {
			t.Fatalf("unexpected values vs=%v, err=%v", vs, e)
		}
		k, v := <-ch, <-ch
		if k != "d" || v != "dd" {
			t.Fatalf("unexpected values k=%v, v=%v", k, v)
		}
		s, l := cache.Size(), cache.Len()
		if s != cacheEntryOverhead*4+2*3+3 || l != 4 {
			t.Fatalf("unexpected values s=%d, l=%d", s, l)
		}
		vs, e = cache.Gets(keys, nil)
		if e != nil || len(vs) != 4 || vs[0] != "a" || vs[1] != "bb" || vs[2] != "c" || vs[3] != "d" {
			t.Fatalf("unexpected values vs=%v, err=%v", vs, e)
		}
	})

	t.Run("should evict after SetMax called", func(t *testing.T) {
		cache := NewCache(10, 1000)
		ch := make(chan interface{}, 2)
		cache.SetOnEvicted(func(key, value interface{}, err error) { ch <- key; ch <- value })
		cache.Get("a", func() (interface{}, time.Duration, int64, error) {
			return "a", time.Minute, 1 + 1, nil // expired entry
		})
		cache.Get("b", func() (interface{}, time.Duration, int64, error) {
			return "b", -time.Minute, 1 + 1, nil // expired entry
		})
		cache.Get("c", func() (interface{}, time.Duration, int64, error) {
			return "c", time.Minute, 1 + 1, nil
		})
		cache.Get("a", nil) // move to head
		hit, miss, expire, evict, replaced := cache.ResetStats()
		if hit != 1 || miss != 3 || expire != 0 || evict != 0 || replaced != 0 {
			t.Fatalf("unexpected stats - hit=%d, miss=%d, expire=%d, evict=%d, replaced=%d", hit, miss, expire, evict, replaced)
		}
		// we should have all three

		cache.SetMax(cache.Len(), cache.Size()) // should not evict anything
		select {
		case <-ch:
			t.Fatalf("evicted too early")
		default:
		}
		hit, miss, expire, evict, replaced = cache.ResetStats()
		if hit != 0 || miss != 0 || expire != 0 || evict != 0 || replaced != 0 {
			t.Fatalf("unexpected stats - hit=%d, miss=%d, expire=%d, evict=%d, replaced=%d", hit, miss, expire, evict, replaced)
		}

		cache.SweepExpired() // should evict the expired entry
		k, v := <-ch, <-ch
		if k != "b" || v != "b" {
			t.Fatalf("unexpected values k=%v, v=%v", k, v)
		}
		hit, miss, expire, evict, replaced = cache.ResetStats()
		if hit != 0 || miss != 0 || expire != 1 || evict != 0 || replaced != 0 {
			t.Fatalf("unexpected stats - hit=%d, miss=%d, expire=%d, evict=%d, replaced=%d", hit, miss, expire, evict, replaced)
		}

		cache.SetMax(cache.Len(), cache.Size()-1) // should evict the oldest (c)
		k, v = <-ch, <-ch
		if k != "c" || v != "c" {
			t.Fatalf("unexpected values k=%v, v=%v", k, v)
		}
		hit, miss, expire, evict, replaced = cache.ResetStats()
		if hit != 0 || miss != 0 || expire != 0 || evict != 1 || replaced != 0 {
			t.Fatalf("unexpected stats - hit=%d, miss=%d, expire=%d, evict=%d, replaced=%d", hit, miss, expire, evict, replaced)
		}

		cache.Purge()
		k, v = <-ch, <-ch
		if k != "a" || v != "a" {
			t.Fatalf("unexpected values k=%v, v=%v", k, v)
		}
		hit, miss, expire, evict, replaced = cache.ResetStats()
		if hit != 0 || miss != 0 || expire != 0 || evict != 1 || replaced != 0 {
			t.Fatalf("unexpected stats - hit=%d, miss=%d, expire=%d, evict=%d, replaced=%d", hit, miss, expire, evict, replaced)
		}
	})

	t.Run("snip on root should panic", func(t *testing.T) {
		cache := NewCache(0, 0)
		defer func() { recover() }()
		cache.snip(cache.root) // should not be possible, but should panic if it happens
		t.Fatalf("unexpected flow - should've paniced in snip")
	})
}

func TestCacheGetRace(t *testing.T) {
	i := 0
	cache := NewCache(100, 10000)
	evictions := int32(0)
	cache.SetOnEvicted(func(key, value interface{}, err error) { atomic.AddInt32(&evictions, 1) })
	ch := make(chan bool)
	gos := 256
	itr := 256
	fetches := int32(0)
	for i < gos {
		go func(i int) {
			defer func() { ch <- true }()
			for j := 0; j < itr; j++ {
				key := fmt.Sprintf("key-%d", (i+j)%47)
				cache.Get(key, func() (interface{}, time.Duration, int64, error) {
					time.Sleep(time.Duration(j%7) * time.Millisecond)
					atomic.AddInt32(&fetches, 1)
					return key, 10 * time.Millisecond, 6 + 6, nil
				})
			}
		}(i)
		i++
	}
	i = 0
	for i < gos {
		<-ch
		i++
	}
	fmt.Printf("fetches = %d / %d, evictions = %d\n", fetches, gos*itr, evictions)
	cache.Purge()
	if cache.Len() != 0 || cache.Size() != 0 {
		t.Fatalf("accounting error, len=%d, size=%d", cache.Len(), cache.Size())
	}
}

func TestCacheGetRace2(t *testing.T) {
	t.Run("random cache Get calls", func(t *testing.T) {
		par, keys, size, dur := 2, 1000, 200, 30
		cache := NewCache(keys*7/10, int64((cacheEntryOverhead+size*5/10)*keys*7/10))
		var wg sync.WaitGroup
		for g := 0; g < par; g++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				for i := 0; i < keys; i++ {
					rk := rand.Intn(keys)
					rs := rand.Intn(size)
					rd := rand.Intn(dur)
					k := "key" + strconv.Itoa(rk)
					cache.Get(k, func() (interface{}, time.Duration, int64, error) {
						return k, time.Millisecond * time.Duration(rd), int64(rs), nil
					})
				}
			}()
		}
		wg.Wait()
		hit, miss, expired, evicted, replaced := cache.Stats()
		fmt.Printf("hit=%d, miss=%d, expired=%d, evicted=%d, replaced=%d\n", hit, miss, expired, evicted, replaced)
		cache.Purge()
		if cache.Len() != 0 || cache.Size() != 0 {
			t.Fatalf("accounting error, len=%d, size=%d", cache.Len(), cache.Size())
		}
	})
}

func TestCacheGetsRace(t *testing.T) {
	i := 0
	cache := NewCache(100, 1000)
	evictions := int32(0)
	cache.SetOnEvicted(func(key, value interface{}, err error) { atomic.AddInt32(&evictions, 1) })
	ch := make(chan bool)
	gos := 256
	itr := 256
	fetches := int32(0)
	gets := int32(0)
	for i < gos {
		go func(i int) {
			defer func() { ch <- true }()

			for j := 0; j < itr; j++ {
				ij := 255 - (i+j)&255
				var keys []interface{}
				if ij&1 != 0 {
					keys = append(keys, "key0")
				}
				if ij&2 != 0 {
					keys = append(keys, "key1")
				}
				if ij&4 != 0 {
					keys = append(keys, "key2")
				}
				if ij&8 != 0 {
					keys = append(keys, "key3")
				}
				if ij&16 != 0 {
					keys = append(keys, "key4")
				}
				if ij&32 != 0 {
					keys = append(keys, "key5")
				}
				if ij&64 != 0 {
					keys = append(keys, "key6")
				}
				if ij&128 != 0 {
					keys = append(keys, "key7")
				}
				cache.Gets(keys, func(keys []interface{}) ([]interface{}, time.Duration, []int64, error) {
					time.Sleep(time.Duration(j%7) * time.Millisecond)
					atomic.AddInt32(&fetches, int32(len(keys)))
					atomic.AddInt32(&gets, 1)
					return keys, 10 * time.Millisecond, []int64{8, 8, 8, 8, 8, 8, 8, 8, 8}, nil
				})
			}
		}(i)
		i++
	}
	i = 0
	for i < gos {
		<-ch
		i++
	}
	fmt.Printf("fetches = %d/%d, gets = %d/%d\n", fetches, gos*itr*4, gets, gos*itr)
	cache.Purge()
	if cache.Len() != 0 || cache.Size() != 0 {
		t.Fatalf("accounting error, len=%d, size=%d", cache.Len(), cache.Size())
	}
}

func BenchmarkCache_Get(b *testing.B) {
	b.Run("benchmark random cache Get calls", func(b *testing.B) {
		cache := NewCache(1000, (cacheEntryOverhead+1500)*100)
		var wg sync.WaitGroup
		par := 10
		itr := b.N / par

		for p := 0; p < par; p++ {
			rnd := rand.NewSource(int64(p))
			wg.Add(1)
			go func() {
				defer wg.Done()
				for i := 0; i < itr; i++ {
					r := rnd.Int63()
					rk := r % 1000
					rs := (r/1000) % 200
					rd := (r/1000/200) % 30
					k := "key" + strconv.Itoa(int(rk))
					cache.Get(k, func() (interface{}, time.Duration, int64, error) {
						return k, time.Millisecond * time.Duration(rd), rs, nil
					})
				}
			}()
		}
		wg.Wait()
		hit, miss, expired, evicted, replaced := cache.Stats()
		fmt.Printf("hit=%d, miss=%d, expired=%d, evicted=%d, replaced=%d\n", hit, miss, expired, evicted, replaced)
		cache.Purge()
		if cache.Len() != 0 || cache.Size() != 0 {
			b.Fatalf("accounting error, len=%d, size=%d", cache.Len(), cache.Size())
		}
	})
}
