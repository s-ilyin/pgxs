package pgxs

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestNewMapping(t *testing.T) {
	cfg := &Config{
		Buckets: 2,
		Shards: []Shard{
			{Name: "shard1", DSN: "..."},
		},
		Mapping: []MappingEntry{
			{Bucket: 0, Shard: "shard1"},
			{Bucket: 1, Shard: "shard1"},
		},
	}

	m, err := newMapping(cfg)
	require.NoError(t, err)
	require.NotNil(t, m)

	shard, err := m.GetShard(0)
	require.NoError(t, err)
	require.Equal(t, "shard1", shard)

	shard, err = m.GetShard(1)
	require.NoError(t, err)
	require.Equal(t, "shard1", shard)

	_, err = m.GetShard(2)
	require.Error(t, err)
	require.Contains(t, err.Error(), "no shard for bucket")
}

func TestMapping_ShardNames(t *testing.T) {
	cfg := &Config{
		Buckets: 1,
		Shards: []Shard{
			{Name: "shard1", DSN: "..."},
			{Name: "shard2", DSN: "..."},
		},
		Mapping: []MappingEntry{
			{Bucket: 0, Shard: "shard1"},
		},
	}
	m, _ := newMapping(cfg)
	names := m.ShardNames()
	require.ElementsMatch(t, []string{"shard1", "shard2"}, names)
}
