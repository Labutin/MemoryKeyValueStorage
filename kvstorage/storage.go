package kvstorage

import (
	"errors"
	"github.com/Labutin/concurrent-map"
	"strconv"
	"time"
)

type Storage struct {
	cmap concurrent_map.CMapInterface
	ttl  concurrent_map.CMapInterface
}

type List []interface{}
type Dict map[string]interface{}

func NewKVStorage(chunks uint32) *Storage {
	kvstorage := &Storage{}
	kvstorage.cmap = concurrent_map.NewCMap(chunks)
	kvstorage.ttl = concurrent_map.NewCMap(10)

	return kvstorage
}

// Set stores value for given key and TTL
func (t *Storage) Set(key string, value interface{}, TTL time.Duration) {
	t.cmap.Put(key, value)
	if TTL > 0 {
		t.ttl.Put(strconv.FormatInt(time.Now().Add(TTL).Unix(), 10), key)
	} else {
		if TTL < 0 {
			t.cmap.Remove(key)
		}
	}
}

// Update updates value for given key
func (t *Storage) Update(key string, value interface{}) error {
	if !t.cmap.IsExist(key) {
		return errors.New("Key not found")
	}
	t.cmap.Put(key, value)
	return nil
}

// Remove deletes value for given key
func (t *Storage) Remove(key string) error {
	return t.cmap.Remove(key)
}

// Get returns value for given key
func (t *Storage) Get(key string) (interface{}, bool) {
	value, ok := t.cmap.Get(key)
	if !ok {
		return nil, false
	}
	tValue := value

	return tValue, true
}

// GetListElement returns i-th element from List value
func (t *Storage) GetListElement(key string, i int) (interface{}, error) {
	value, ok := t.Get(key)
	if !ok {
		return nil, errors.New("Key not found")
	}
	if vl, ok := value.(List); !ok {
		return nil, errors.New("Value not List")
	} else {
		if len(vl) <= i {
			return nil, errors.New("Out of bound")
		}
		return vl[i], nil
	}
}

func (t *Storage) GetDictElement(key, dictKey string) (interface{}, error) {
	value, ok := t.Get(key)
	if !ok {
		return nil, errors.New("Key not found")
	}
	if vl, ok := value.(Dict); !ok {
		return nil, errors.New("Value not List")
	} else {
		if value, ok := vl[dictKey]; ok {
			return value, nil
		} else {
			return nil, errors.New("Key in dictionary not found")
		}
	}
}

// Keys returns all keys in map
func (t *Storage) Keys() []string {
	return t.cmap.Keys()
}
