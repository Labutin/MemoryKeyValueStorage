package kvstorage

import (
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"testing"
)

func TestStorage_GetSet(t *testing.T) {
	storage := NewKVStorage(10)
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
	storage := NewKVStorage(10)
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

func TestKVStorage_GetListElement(t *testing.T) {
	storage := NewKVStorage(10)
	m := []int{0, 1, 2, 3, 4}
	storage.Set("test", MakeListFromInts(m), 0)
	v, ok := storage.Get("test")
	assert.True(t, ok)
	assert.Equal(t, MakeListFromInts(m), v)
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
