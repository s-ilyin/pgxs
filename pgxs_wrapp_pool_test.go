package pgxs

import (
	"context"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/stretchr/testify/require"
)

// --- Тесты PoolOption ---

func TestPoolOptions(t *testing.T) {
	t.Parallel()

	t.Run("WithPoolMaxConns", func(t *testing.T) {
		t.Parallel()
		cfg, err := pgxpool.ParseConfig("postgres://localhost:5432/db")
		require.NoError(t, err)

		WithPoolMaxConns(10)(cfg)
		require.Equal(t, int32(10), cfg.MaxConns)

		// отрицательное значение не меняет конфиг
		WithPoolMaxConns(-1)(cfg)
		require.Equal(t, int32(10), cfg.MaxConns)
	})

	t.Run("WithPoolMinConns", func(t *testing.T) {
		t.Parallel()
		cfg, err := pgxpool.ParseConfig("postgres://localhost:5432/db")
		require.NoError(t, err)

		WithPoolMinConns(5)(cfg)
		require.Equal(t, int32(5), cfg.MinConns)

		// ноль допустим
		WithPoolMinConns(0)(cfg)
		require.Equal(t, int32(0), cfg.MinConns)

		// отрицательное значение игнорируется
		WithPoolMinConns(-5)(cfg)
		require.Equal(t, int32(0), cfg.MinConns)
	})

	t.Run("WithPoolMaxConnIdleTime", func(t *testing.T) {
		t.Parallel()
		cfg, err := pgxpool.ParseConfig("postgres://localhost:5432/db")
		require.NoError(t, err)

		dur := 5 * time.Minute
		WithPoolMaxConnIdleTime(dur)(cfg)
		require.Equal(t, dur, cfg.MaxConnIdleTime)

		// отрицательное значение игнорируется
		WithPoolMaxConnIdleTime(-time.Second)(cfg)
		require.Equal(t, dur, cfg.MaxConnIdleTime)
	})

	t.Run("WithPoolHealthCheckPeriod", func(t *testing.T) {
		t.Parallel()
		cfg, err := pgxpool.ParseConfig("postgres://localhost:5432/db")
		require.NoError(t, err)

		dur := 30 * time.Second
		WithPoolHealthCheckPeriod(dur)(cfg)
		require.Equal(t, dur, cfg.HealthCheckPeriod)
	})

	t.Run("WithPoolMaxConnLifetime", func(t *testing.T) {
		t.Parallel()
		cfg, err := pgxpool.ParseConfig("postgres://localhost:5432/db")
		require.NoError(t, err)

		dur := 1 * time.Hour
		WithPoolMaxConnLifetime(dur)(cfg)
		require.Equal(t, dur, cfg.MaxConnLifetime)
	})

	t.Run("WithPoolConnConfig", func(t *testing.T) {
		t.Parallel()
		cfg, err := pgxpool.ParseConfig("postgres://localhost:5432/db")
		require.NoError(t, err)

		called := false
		WithPoolConnConfig(func(c *pgxpool.Config) {
			called = true
			c.MaxConns = 99
		})(cfg)

		require.True(t, called)
		require.Equal(t, int32(99), cfg.MaxConns)
	})
}

// --- Тесты GetPool ---

func TestWrapPool_GetPool(t *testing.T) {
	t.Parallel()

	t.Run("returns pool for existing shard", func(t *testing.T) {
		t.Parallel()
		mockPool := NewRetryPoolMock(t)
		wp := &wrapPool{
			pools: map[string]RetryPool{
				"shard_1": mockPool,
			},
		}

		pool, err := wp.GetPool("shard_1")
		require.NoError(t, err)
		require.Same(t, mockPool, pool)
	})

	t.Run("returns error for missing shard", func(t *testing.T) {
		t.Parallel()
		wp := &wrapPool{
			pools: map[string]RetryPool{},
		}

		pool, err := wp.GetPool("unknown")
		require.Error(t, err)
		require.Nil(t, pool)
		require.Contains(t, err.Error(), `shard "unknown" not found`)
	})
}

// --- Тесты Close ---

func TestWrapPool_Close(t *testing.T) {
	t.Parallel()

	t.Run("closes all pools", func(t *testing.T) {
		t.Parallel()
		pool1 := NewRetryPoolMock(t)
		pool2 := NewRetryPoolMock(t)

		pool1.EXPECT().Close().Once()
		pool2.EXPECT().Close().Once()

		wp := &wrapPool{
			pools: map[string]RetryPool{
				"shard_1": pool1,
				"shard_2": pool2,
			},
		}

		wp.Close()
		require.Nil(t, wp.pools) // карта очищена
	})

	t.Run("close empty wrapPool is safe", func(t *testing.T) {
		t.Parallel()
		wp := &wrapPool{
			pools: map[string]RetryPool{},
		}
		wp.Close()
		require.Nil(t, wp.pools)
	})
}

// --- Тесты newPool (требуют реальную БД) ---

func TestNewPool(t *testing.T) {
	t.Parallel()

	t.Run("fails on invalid DSN", func(t *testing.T) {
		t.Parallel()
		cfg := &Config{
			Buckets:      4,
			SchemaPrefix: "bucket_",
			Shards: []Shard{
				{Name: "shard_1", DSN: "invalid-dsn://%%%"},
			},
			Mapping: []MappingEntry{
				{Bucket: 0, Shard: "shard_1"},
				{Bucket: 1, Shard: "shard_1"},
				{Bucket: 2, Shard: "shard_1"},
				{Bucket: 3, Shard: "shard_1"},
			},
		}

		pool, err := newPool(context.Background(), cfg)
		require.Error(t, err)
		require.Nil(t, pool)
		require.Contains(t, err.Error(), "failed to parse DSN")
	})

	t.Run("fails on unreachable shard", func(t *testing.T) {
		t.Parallel()
		cfg := &Config{
			Buckets:      4,
			SchemaPrefix: "bucket_",
			Shards: []Shard{
				{Name: "shard_1", DSN: "postgres://user:pass@localhost:1/db"},
			},
			Mapping: []MappingEntry{
				{Bucket: 0, Shard: "shard_1"},
				{Bucket: 1, Shard: "shard_1"},
				{Bucket: 2, Shard: "shard_1"},
				{Bucket: 3, Shard: "shard_1"},
			},
		}

		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()

		pool, err := newPool(ctx, cfg)
		require.Error(t, err)
		require.Nil(t, pool)
	})
}
