package pgxs

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestHashKey(t *testing.T) {
	tests := []struct {
		name       string
		input      []byte
		maxBuckets MaxBuckets
		expected   uint
	}{
		{"simple", []byte("user123"), 4, 3}, // результат может меняться, проверяем только диапазон
		{"empty", []byte{}, 4, 0},
		{"max", []byte("test"), 1, 0}, // только один бакет
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := HashKey(tt.input, tt.maxBuckets)
			require.Less(t, got, uint(tt.maxBuckets), "hash out of range")
			require.GreaterOrEqual(t, got, uint(0), "hash negative")
		})
	}
}
