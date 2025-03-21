package elastichash_test

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	elastichash "github.com/jaronoff97/elastic-hash"
)

func TestNewHashTable(t *testing.T) {
	ht := elastichash.NewHashTable[string, int](100, 0.1)
	assert.NotNil(t, ht)
}

func TestHashTable(t *testing.T) {
	tests := []struct {
		name          string
		capacity      int
		delta         float64
		operations    []operation
		expectedError error
		expectedGet   map[string]int
	}{
		{
			name:     "Basic Insert and Get",
			capacity: 10,
			delta:    0.1,
			operations: []operation{
				{opType: "insert", key: "key1", value: 1},
				{opType: "insert", key: "key2", value: 2},
				{opType: "insert", key: "key3", value: 3},
			},
			expectedError: nil,
			expectedGet: map[string]int{
				"key1": 1,
				"key2": 2,
				"key3": 3,
			},
		},
		{
			name:     "Insert into full table",
			capacity: 3,
			delta:    0.1,
			operations: []operation{
				{opType: "insert", key: "key1", value: 1},
				{opType: "insert", key: "key2", value: 2},
				{opType: "insert", key: "key3", value: 3},
				{opType: "insert", key: "key4", value: 4},
			},
			expectedError: elastichash.FailedToInsertErr,
			expectedGet: map[string]int{
				"key1": 1,
				"key2": 2,
				"key3": 3,
			},
		},
		{
			name:     "Get non-existent key",
			capacity: 10,
			delta:    0.1,
			operations: []operation{
				{opType: "insert", key: "key1", value: 1},
				{opType: "insert", key: "key2", value: 2},
			},
			expectedError: nil,
			expectedGet: map[string]int{
				"key1": 1,
				"key2": 2,
				"key3": 0, // non-existent key should return zero value
			},
		},
		{
			name:     "Insert duplicate key",
			capacity: 10,
			delta:    0.1,
			operations: []operation{
				{opType: "insert", key: "key1", value: 1},
				{opType: "insert", key: "key1", value: 2},
			},
			expectedError: nil,
			expectedGet: map[string]int{
				"key1": 2, // last inserted value should be returned
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ht := elastichash.NewHashTable[string, int](tt.capacity, tt.delta)
			var err error
			for _, op := range tt.operations {
				switch op.opType {
				case "insert":
					err = ht.Insert(op.key, op.value)
				}
				if err != nil {
					break
				}
			}
			require.Equal(t, tt.expectedError, err)

			for key, expectedValue := range tt.expectedGet {
				value, found := ht.Get(key)
				if expectedValue == 0 {
					assert.False(t, found)
				} else {
					assert.True(t, found)
					assert.Equal(t, expectedValue, value)
				}
			}
		})
	}
}

func TestBasic(t *testing.T) {
	capacity := 10
	delta := 0.1
	ht := elastichash.NewHashTable[string, int](capacity, delta)
	err := ht.Insert("example", 1)
	assert.NoError(t, err)
	err = ht.Insert("example2", 1)
	assert.NoError(t, err)
	v, ok := ht.Get("example")
	assert.True(t, ok)
	assert.Equal(t, 1, v)
	v, ok = ht.Get("example2")
	assert.True(t, ok)
	assert.Equal(t, 1, v)
	fmt.Println(ht.String())
}

type operation struct {
	opType string
	key    string
	value  int
}
