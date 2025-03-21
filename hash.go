package elastichash

import (
	"errors"
	"fmt"
	"hash/maphash"
	"math"
	"strconv"
	"strings"
)

var (
	OutOfSpaceErr     = errors.New("out of space, hash table is full")
	FailedToInsertErr = errors.New("failed to insert to hash table")
)

const (
	threshold = 0.25
)

type ValidKey interface {
	comparable
	~[]byte | ~int | ~string
}

func HashKey[K ValidKey](k K) uint64 {
	var h maphash.Hash
	switch v := any(k).(type) {
	case ([]byte):
		h.Write(v)
	case (int):
		h.WriteString(strconv.Itoa(v))
	case (string):
		h.WriteString(v)
	}
	return h.Sum64()
}

type entry[K ValidKey, V any] struct {
	key   K
	value V
}

type HashTable[K ValidKey, V any] struct {
	capacity int
	delta    float64

	items              int
	levels             [][]*entry[K, V]
	occupanciesByLevel []int
	c                  float64
}

func NewHashTable[K ValidKey, V any](capacity int, delta float64) *HashTable[K, V] {
	ht := &HashTable[K, V]{
		capacity: capacity,
		delta:    delta,
		items:    0,
		c:        4,
	}
	ht.clear()
	return ht
}

func (ht *HashTable[K, V]) clear() {
	numLevels := math.Max(1, math.Floor(math.Log2(float64(ht.capacity))))
	remaining := float64(ht.capacity)
	sizes := []int{}
	for i := range int(numLevels - 1) {
		size := math.Max(1, math.Floor(remaining/math.Pow(2, numLevels-float64(i))))
		sizes = append(sizes, int(size))
		remaining -= size
	}
	ht.levels = make([][]*entry[K, V], int(numLevels))
	ht.occupanciesByLevel = make([]int, int(numLevels))
	for i, s := range sizes {
		ht.levels[i] = make([]*entry[K, V], s)
		ht.occupanciesByLevel[i] = 0
	}
}

func (ht *HashTable[K, V]) maxLen() int {
	return ht.capacity - int(ht.delta*float64(ht.capacity))
}

func (ht *HashTable[K, V]) probe(key K, j int64, size int) int {
	masked := HashKey(key) & 0xFFFFFFFF
	return int(int64(masked)+j*j) % size
}

func (ht *HashTable[K, V]) Insert(key K, value V) error {
	if ht.items >= ht.maxLen() {
		return OutOfSpaceErr
	}
	for i, l := range ht.levels {
		size := len(l)
		freeOnLevel := size - ht.occupanciesByLevel[i]
		load := float64(freeOnLevel) / float64(size)
		probeLimit := int64(math.Max(1, ht.c*math.Min(math.Log2(math.Max(1/load, 0)), math.Log2(1/ht.delta))))
		if i < len(ht.levels)-1 {
			nextLevel := ht.levels[i+1]
			nextOccupancy := ht.occupanciesByLevel[i+1]
			nextFreeOnLevel := float64(len(nextLevel) - nextOccupancy)
			nextLoad := float64(0)
			if len(nextLevel) > 0 {
				nextLoad = nextFreeOnLevel / float64(len(nextLevel))
			}
			if load > (ht.delta/2) && nextLoad > threshold {
				for j := range probeLimit {
					idx := ht.probe(key, j, size)
					if l[idx] == nil {
						l[idx] = &entry[K, V]{key, value}
						ht.occupanciesByLevel[i] += 1
						ht.items += 1
						return nil
					}
				}
			} else if load <= (ht.delta / 2) {
				continue
			} else if nextLoad <= threshold {
				for j := range probeLimit {
					idx := ht.probe(key, j, size)
					if l[idx] == nil {
						l[idx] = &entry[K, V]{key, value}
						ht.occupanciesByLevel[i] += 1
						ht.items += 1
						return nil
					}
				}
			}
		} else {
			for j := range probeLimit {
				idx := ht.probe(key, j, size)
				if l[idx] == nil {
					l[idx] = &entry[K, V]{key, value}
					ht.occupanciesByLevel[i] += 1
					ht.items += 1
					return nil
				}
			}
		}
	}
	return FailedToInsertErr
}

func (ht *HashTable[K, V]) Get(key K) (V, bool) {
	toReturn := new(V)
	for i, level := range ht.levels {
		size := len(level)
		freeOnLevel := size - ht.occupanciesByLevel[i]
		load := float64(freeOnLevel) / float64(size)
		probeLimit := int64(math.Max(1, ht.c*math.Min(math.Log2(math.Max(1/load, 0)), math.Log2(1/ht.delta))))
		for j := range probeLimit {
			idx := ht.probe(key, int64(j), size)
			if level[idx] == nil {
				continue
			} else if level[idx].key == key {
				return level[idx].value, true
			}
		}
	}
	return *toReturn, false
}

func (ht *HashTable[K, V]) String() string {
	var sb strings.Builder
	sb.WriteString("{")
	for _, level := range ht.levels {
		for _, e := range level {
			if e != nil {
				sb.WriteString("\"")
				sb.WriteString(fmt.Sprintf("%v", e.key))
				sb.WriteString("\"")
				sb.WriteString(": ")
				sb.WriteString("\"")
				sb.WriteString(fmt.Sprintf("%v", e.value))
				sb.WriteString("\"")
				sb.WriteString(", ")
			}
		}
	}

	sb.WriteString("}")
	return sb.String()
}
