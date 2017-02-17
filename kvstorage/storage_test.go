package kvstorage

import (
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"sort"
	"strconv"
	"testing"
	"time"
)

func TestStorage_GetSet(t *testing.T) {
	storage := NewKVStorage(10, true)
	storage.Set("test123", 123, 0)
	storage.Set("test124", 124, 0)
	v, ok := storage.Get("test123")
	require.True(t, ok)
	require.Equal(t, 123, v)
	v, ok = storage.Get("test124")
	require.True(t, ok)
	require.Equal(t, 124, v)
	v, ok = storage.Get("test125")
	require.False(t, ok)
	require.Nil(t, v)
	storage.Set("test123", 200, 0)
	v, ok = storage.Get("test123")
	require.True(t, ok)
	require.Equal(t, 200, v)
}

func TestStorage_Remove(t *testing.T) {
	storage := NewKVStorage(10, true)
	storage.Set("test123", 123, 0)
	storage.Set("test124", 124, 0)
	v, ok := storage.Get("test123")
	require.True(t, ok)
	require.Equal(t, 123, v)
	v, ok = storage.Get("test124")
	require.True(t, ok)
	require.Equal(t, 124, v)
	storage.Remove("test124")
	v, ok = storage.Get("test123")
	require.True(t, ok)
	require.Equal(t, 123, v)
	v, ok = storage.Get("test124")
	require.False(t, ok)
	require.Nil(t, v)
}

func TestStorage_Update(t *testing.T) {
	storage := NewKVStorage(10, true)
	storage.Set("test123", 123, 0)
	v, ok := storage.Get("test123")
	require.True(t, ok)
	require.Equal(t, 123, v)
	err := storage.Update("notexists", 1)
	require.Error(t, err)
	err = storage.Update("test123", 1)
	require.NoError(t, err)
	v, ok = storage.Get("test123")
	require.True(t, ok)
	require.Equal(t, 1, v)
}

func TestStorage_Keys(t *testing.T) {
	storage := NewKVStorage(10, true)
	keys := []string{}
	for i := 0; i < 100; i++ {
		keys = append(keys, strconv.Itoa(i))
		storage.Set(keys[i], i, 0)
	}
	retKeys := storage.Keys()
	sort.Strings(retKeys)
	sort.Strings(keys)
	require.Equal(t, keys, retKeys)

}

func TestKVStorage_GetListElement(t *testing.T) {
	storage := NewKVStorage(10, true)
	m := []interface{}{0, 1, 2, 3, 4}
	storage.Set("test", m, 0)
	v, ok := storage.Get("test")
	assert.True(t, ok)
	assert.Equal(t, m, v)
	for i := 0; i < 5; i++ {
		val, err := storage.GetListElement("test", i)
		require.Nil(t, err)
		require.Equal(t, i, val)
	}
	v, err := storage.GetListElement("test", 10)
	assert.Error(t, err)
	assert.Nil(t, v)
	storage.Set("testNotList", 123, 0)
	v, err = storage.GetListElement("testNotList", 1)
	assert.Error(t, err)
	assert.Nil(t, v)
}

func TestStorage_GetDictElement(t *testing.T) {
	storage := NewKVStorage(10, true)
	dict := map[string]interface{}{}
	dict["t1"] = 1
	dict["t2"] = 2
	storage.Set("key", dict, 0)
	value, err := storage.GetDictElement("key", "t1")
	require.NoError(t, err)
	require.Equal(t, 1, value)
	value, err = storage.GetDictElement("key", "t2")
	require.NoError(t, err)
	require.Equal(t, 2, value)
	value, err = storage.GetDictElement("key", "absent")
	require.Error(t, err)
	require.Nil(t, value)
}

func TestStorage_TTL(t *testing.T) {
	storage := NewKVStorage(10, false)
	storage.ttlTimeout = time.Second * 1
	storage.startTTLProcessing()
	storage.Set("t1", 1, time.Microsecond*10)
	storage.Set("t2", 2, time.Second*2)
	storage.Set("t3", 3, time.Second*2)
	v, ok := storage.Get("t1")
	require.True(t, ok)
	require.Equal(t, v, 1)
	v, ok = storage.Get("t2")
	require.True(t, ok)
	require.Equal(t, v, 2)
	time.Sleep(time.Second * 2)
	v, ok = storage.Get("t1")
	require.False(t, ok)
	require.Nil(t, v)
	v, ok = storage.Get("t2")
	require.True(t, ok)
	require.Equal(t, v, 2)
	time.Sleep(time.Second * 2)
	v, ok = storage.Get("t1")
	require.False(t, ok)
	require.Nil(t, v)
	v, ok = storage.Get("t2")
	require.False(t, ok)
	require.Nil(t, v)
}
