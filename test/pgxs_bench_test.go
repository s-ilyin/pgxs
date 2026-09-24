//go:build pgxs_integration

package test

import (
	"context"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/s-ilyin/pgxs"
	"github.com/stretchr/testify/require"
)

var benchSizes = []int{100, 500, 1000, 2500, 5000, 6500, 8000, 10000}

const (
	envSyncOn1Shard  = "PG_SYNC_ON_1SHARD_DSN"
	envSyncOff1Shard = "PG_SYNC_OFF_1SHARD_DSN"
	envSyncOnMulti1  = "PG_SYNC_ON_MULTI_1_DSN"
	envSyncOnMulti2  = "PG_SYNC_ON_MULTI_2_DSN"
	envSyncOffMulti1 = "PG_SYNC_OFF_MULTI_1_DSN"
	envSyncOffMulti2 = "PG_SYNC_OFF_MULTI_2_DSN"
)

func setupBenchClient(b *testing.B, dsn1, dsn2 string, batchTx bool) (*pgxs.Client, map[pgxs.BucketID]*pgxpool.Pool) {
	b.Helper()

	if dsn1 == "" {
		b.Skip("first DSN is not set")
	}
	multi := dsn2 != ""

	shards := []pgxs.Shard{{Name: "shard_1", DSN: dsn1}}
	mapping := []pgxs.MappingEntry{
		{Bucket: 0, Shard: "shard_1"},
		{Bucket: 1, Shard: "shard_1"},
		{Bucket: 2, Shard: "shard_1"},
		{Bucket: 3, Shard: "shard_1"},
	}
	if multi {
		shards = append(shards, pgxs.Shard{Name: "shard_2", DSN: dsn2})
		mapping = []pgxs.MappingEntry{
			{Bucket: 0, Shard: "shard_1"},
			{Bucket: 1, Shard: "shard_1"},
			{Bucket: 2, Shard: "shard_2"},
			{Bucket: 3, Shard: "shard_2"},
		}
	}

	cfg := &pgxs.Config{
		Buckets:      4,
		SchemaPrefix: "bucket_",
		Shards:       shards,
		Mapping:      mapping,
	}

	client, err := pgxs.New(
		context.Background(), cfg,
		pgxs.WithConcurrency(4),
		pgxs.WithBatchTx(batchTx),
	)
	require.NoError(b, err)

	p1, err := pgxpool.New(context.Background(), dsn1)
	require.NoError(b, err)

	buckets := map[pgxs.BucketID]*pgxpool.Pool{0: p1, 1: p1, 2: p1, 3: p1}

	var p2 *pgxpool.Pool
	if multi {
		p2, err = pgxpool.New(context.Background(), dsn2)
		require.NoError(b, err)
		buckets[2] = p2
		buckets[3] = p2
	}

	b.Cleanup(func() {
		p1.Close()
		if p2 != nil {
			p2.Close()
		}
		client.Close()
	})

	return client, buckets
}

// cleanupBenchData очищает все таблицы users во всех бакетах.
func cleanupBenchData(b *testing.B, buckets map[pgxs.BucketID]*pgxpool.Pool) {
	b.Helper()
	for bucketID, pool := range buckets {
		q := fmt.Sprintf("TRUNCATE TABLE bucket_%d.users CASCADE", bucketID)
		_, err := pool.Exec(context.Background(), q)
		require.NoError(b, err)
	}
}

// buildBenchUsers строит N пользователей с ключами, дающими указанный bucketID.
// benchTag — уникальный префикс (например, "syncon1shard"), чтобы ID не пересекались
// между разными бенчмарками.
func buildBenchUsers(benchTag string, fixedBucket, count, iter int) []testUser {
	users := make([]testUser, 0, count)

	for i := 0; len(users) < count; i++ {
		id := KeyShardID(fmt.Sprintf("bench-%s-%d-%d", benchTag, iter, i))
		bucket := int(pgxs.HashKey(id.Prehash(), 4))

		if fixedBucket >= 0 && bucket != fixedBucket {
			continue
		}
		users = append(users, testUser{
			ID:   id,
			Name: "name",
			Age:  i % 100,
		})
	}
	return users
}

func insertQuery(b pgxs.BucketID, u testUser) (string, []any) {
	return `INSERT INTO {schema}.users (id, name, age) VALUES ($1, $2, $3) RETURNING id`,
		[]any{u.ID, u.Name, u.Age}
}

func scanID(rows pgx.Rows) (string, error) {
	var id string
	err := rows.Scan(&id)
	return id, err
}

func pct(d, total time.Duration) float64 {
	if total == 0 {
		return 0
	}
	return 100 * float64(d) / float64(total)
}

// runInsertBench — общая логика бенчмарка.
// benchTag — уникальный префикс, отличающий данные этого бенчмарка от других.
func runInsertBench(b *testing.B, benchTag string, fixedBucket int, dsn1, dsn2 string, batchTx bool) {
	client, buckets := setupBenchClient(b, dsn1, dsn2, batchTx)

	for _, n := range benchSizes {
		n := n
		b.Run(fmt.Sprintf("n=%d", n), func(b *testing.B) {
			// Чистим остатки от предыдущих прогонов ДО timed loop.
			cleanupBenchData(b, buckets)

			b.ReportAllocs()
			b.ResetTimer()

			iter := 0
			for i := 0; i < b.N; i++ {
				iter++
				users := buildBenchUsers(benchTag, fixedBucket, n, iter)

				_, err := pgxs.Query(
					context.Background(), client, users,
					func(u testUser) []byte { return u.ID.Prehash() },
					insertQuery,
					scanID,
				)
				require.NoError(b, err)
			}

			// Чистим после timed loop — вне измерений.
			b.StopTimer()
			cleanupBenchData(b, buckets)
			b.StartTimer()

			totalItems := float64(n) * float64(b.N)
			b.ReportMetric(totalItems/b.Elapsed().Seconds(), "items/s")
		})
	}
}

// BenchmarkInsert_SyncOn_1Shard — 1 шард, synchronous_commit=on.
func BenchmarkInsert_SyncOn_1ShardTxOn(b *testing.B) {
	runInsertBench(b, "sync_on_1", -1, os.Getenv(envSyncOn1Shard), "", true)
}

func BenchmarkInsert_SyncOn_1ShardTxOff(b *testing.B) {
	runInsertBench(b, "sync_on_1_off", -1, os.Getenv(envSyncOn1Shard), "", false)
}

// BenchmarkInsert_SyncOff_1Shard — 1 шард, synchronous_commit=off.
func BenchmarkInsert_SyncOff_1ShardTxOn(b *testing.B) {
	runInsertBench(b, "sync_off_1", -1, os.Getenv(envSyncOff1Shard), "", true)
}

func BenchmarkInsert_SyncOff_1ShardTxOff(b *testing.B) {
	runInsertBench(b, "sync_off_1_tx_off", -1, os.Getenv(envSyncOff1Shard), "", false)
}

// BenchmarkInsert_SyncOn_2Shards — 2 шарда, synchronous_commit=on.
func BenchmarkInsert_SyncOn_2ShardsTxOn(b *testing.B) {
	runInsertBench(b, "sync_on_2", -1, os.Getenv(envSyncOnMulti1), os.Getenv(envSyncOnMulti2), true)
}

func BenchmarkInsert_SyncOn_2ShardsTxOff(b *testing.B) {
	runInsertBench(b, "sync_on_2_tx_off", -1, os.Getenv(envSyncOnMulti1), os.Getenv(envSyncOnMulti2), false)
}

// BenchmarkInsert_SyncOff_2Shards — 2 шарда, synchronous_commit=off.
func BenchmarkInsert_SyncOff_2ShardsTxOn(b *testing.B) {
	runInsertBench(b, "sync_off_2", -1, os.Getenv(envSyncOffMulti1), os.Getenv(envSyncOffMulti2), true)
}

func BenchmarkInsert_SyncOff_2ShardsTxOff(b *testing.B) {
	runInsertBench(b, "sync_off_2_tx_off", -1, os.Getenv(envSyncOffMulti1), os.Getenv(envSyncOffMulti2), false)
}
