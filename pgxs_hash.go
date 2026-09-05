package pgxs

import (
	"github.com/cespare/xxhash/v2"
)

type PreHasher interface {
	PreHash() []byte
}

// HashKey вычисляет номер бакета для строкового ключа.
func HashKey(preKeyHash []byte, maxBuckets MaxBuckets) uint {
	h := xxhash.Sum64(preKeyHash)
	return uint(h % uint64(maxBuckets))
}
