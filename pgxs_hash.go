package pgxs

import (
	"github.com/cespare/xxhash/v2"
)

type Prehasher interface {
	Prehash() []byte
}

func BucketIdFromHash(preKeyHash []byte, maxBuckets MaxBuckets) BucketID {
	return BucketID(HashKey(preKeyHash, maxBuckets))
}

// HashKey вычисляет номер бакета для строкового ключа.
func HashKey(preKeyHash []byte, maxBuckets MaxBuckets) uint {
	h := xxhash.Sum64(preKeyHash)
	return uint(h % uint64(maxBuckets))
}

func (c *Client) BucketIdFromHash(prehash []byte) BucketID {
	return BucketIdFromHash(prehash, c.config.Buckets)
}
