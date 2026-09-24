package pgxs

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestConfig_Validate(t *testing.T) {
	t.Run("valid config", func(t *testing.T) {
		cfg := &Config{
			Buckets:      4,
			SchemaPrefix: "bucket_",
			Shards: []Shard{
				{Name: "shard_1", DSN: "postgres://user:pass@localhost:5432/db"},
			},
			Mapping: []BucketMapping{
				{Bucket: 0, Shard: "shard_1"},
				{Bucket: 1, Shard: "shard_1"},
				{Bucket: 2, Shard: "shard_1"},
				{Bucket: 3, Shard: "shard_1"},
			},
		}
		err := cfg.Validate()
		require.NoError(t, err)
	})

	t.Run("missing mapping", func(t *testing.T) {
		cfg := &Config{
			Buckets:      2,
			SchemaPrefix: "bucket_",
			Shards: []Shard{
				{Name: "shard_1", DSN: "postgres://..."},
			},
			Mapping: []BucketMapping{},
		}
		err := cfg.Validate()
		require.Error(t, err)
		require.Contains(t, err.Error(), "mapping must be explicitly defined")
	})

	t.Run("invalid bucket mapping", func(t *testing.T) {
		cfg := &Config{
			Buckets:      2,
			SchemaPrefix: "bucket_",
			Shards: []Shard{
				{Name: "shard_1", DSN: "postgres://..."},
			},
			Mapping: []BucketMapping{
				{Bucket: 0, Shard: "shard_1"},
				{Bucket: 2, Shard: "shard_1"}, // bucket out of range
			},
		}
		err := cfg.Validate()
		require.Error(t, err)
		require.Contains(t, err.Error(), "bucket 2 out of range")
	})

	t.Run("shard not found in mapping", func(t *testing.T) {
		cfg := &Config{
			Buckets:      1,
			SchemaPrefix: "bucket_",
			Shards: []Shard{
				{Name: "shard_1", DSN: "postgres://..."},
			},
			Mapping: []BucketMapping{
				{Bucket: 0, Shard: "shard_2"},
			},
		}
		err := cfg.Validate()
		require.Error(t, err)
		require.Contains(t, err.Error(), "shard \"shard_2\" not found")
	})
}
