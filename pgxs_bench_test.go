package pgxs

import (
	"strconv"
	"testing"

	"github.com/stretchr/testify/require"
)

// BenchmarkBucketIdFromHash измеряет стоимость хеширования ключа и вычисления bucketID.
// Вызывается на каждый элемент в Query/Exec/ForEachRow — это hot path.
func BenchmarkBucketIdFromHash(b *testing.B) {
	key := []byte("user_12345")
	const buckets MaxBuckets = 256

	b.ReportAllocs()

	for b.Loop() {
		_ = BucketIdFromHash(key, buckets)
	}
}

// BenchmarkClientResolve измеряет стоимость определения шарда по bucketID
// и формирования имени схемы. Вызывается на каждый запрос.
func BenchmarkClientResolve(b *testing.B) {
	const buckets = 256

	cfg := &Config{
		Buckets:      buckets,
		SchemaPrefix: "bucket_",
		Shards: []Shard{
			{Name: "shard_1", DSN: "postgres://localhost:5432/db"},
		},
		Mapping: make([]MappingEntry, buckets),
	}
	for i := range buckets {
		cfg.Mapping[i] = MappingEntry{Bucket: BucketID(i), Shard: "shard_1"}
	}

	m, err := newMapping(cfg)
	require.NoError(b, err)

	c := &Client{
		config:      cfg,
		mapping:     m,
		schemaNames: make([]string, cfg.Buckets),
	}
	for i := range int(cfg.Buckets) {
		c.schemaNames[i] = cfg.SchemaPrefix + strconv.Itoa(i)
	}

	b.ReportAllocs()

	for i := 0; b.Loop(); i++ {
		_, _, _ = c.resolve(BucketID(i % buckets))
	}
}
