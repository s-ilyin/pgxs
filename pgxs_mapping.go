package pgxs

import (
	"fmt"

	
)

type Mapping interface {
	GetShard(bucketID BucketID) (string, error)
}

type mapping struct {
	bucketToShard map[BucketID]string
	shardNames    []string
}

func newMapping(cfg *Config) (*mapping, error) {
	if len(cfg.Shards) == 0 {
		return nil, fmt.Errorf("no shards")
	}
	if len(cfg.Mapping) == 0 {
		return nil, fmt.Errorf("mapping is empty")
	}

	m := &mapping{
		bucketToShard: make(map[BucketID]string, cfg.Buckets),
		shardNames:    make([]string, len(cfg.Shards)),
	}
	for i, sh := range cfg.Shards {
		m.shardNames[i] = sh.Name
	}

	for _, entry := range cfg.Mapping {
		m.bucketToShard[entry.Bucket] = entry.Shard
	}
	return m, nil
}

func (m *mapping) GetShard(bucketID BucketID) (string, error) {
	shard, ok := m.bucketToShard[bucketID]
	if !ok {
		return "", fmt.Errorf("no shard for bucket %d", bucketID)
	}
	return shard, nil
}
