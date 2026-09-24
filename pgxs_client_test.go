package pgxs

import (
	"strconv"
	"testing"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/s-ilyin/pgxs/mocks"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type clientMock struct {
	*Client
	poolMock           *RetryPoolMock
	connMock           *RetryConnMock
	txMock             *TxMock
	prehashMock        *PreHasherMock
	wrapPoolMock       *WrapPoolMock
	mappingMock        *MappingMock
	pgxTxMock          *mocks.PgxTxMock
	pgxBatchResultMock *mocks.PgxBatchResultsMock
	pgxRowMock         *mocks.PgxRowMock
	pgxRowsMock        *mocks.PgxRowsMock
}

func clientMocks(t *testing.T) *clientMock {
	var (
		poolMock           = NewRetryPoolMock(t)
		connMock           = NewRetryConnMock(t)
		txMock             = NewTxMock(t)
		prehashMock        = NewPreHasherMock(t)
		wrapPoolMock       = NewWrapPoolMock(t)
		mappingMock        = NewMappingMock(t)
		pgxTxMock          = mocks.NewPgxTxMock(t)
		pgxBatchResultMock = mocks.NewPgxBatchResultsMock(t)
		pgxRowMock         = mocks.NewPgxRowMock(t)
		pgxRowsMock        = mocks.NewPgxRowsMock(t)
	)
	client := &Client{
		mapping:  mappingMock,
		wrapPool: wrapPoolMock,
		config: &Config{
			Buckets: 4,
			Retry: RetryConfig{
				MaxAttempts: 1,
			},
			SchemaPrefix: "bucket_",
		},
		concurrency: 2, // по умолчанию для тестов ставим 2
		schemaNames: make([]string, 100),
	}
	for i := range int(client.config.Buckets) {
		client.schemaNames[i] = client.config.SchemaPrefix + strconv.Itoa(i)
	}

	return &clientMock{
		Client:             client,
		wrapPoolMock:       wrapPoolMock,
		mappingMock:        mappingMock,
		poolMock:           poolMock,
		connMock:           connMock,
		txMock:             txMock,
		prehashMock:        prehashMock,
		pgxTxMock:          pgxTxMock,
		pgxBatchResultMock: pgxBatchResultMock,
		pgxRowMock:         pgxRowMock,
		pgxRowsMock:        pgxRowsMock,
	}
}

type mockRow struct {
	scanFunc func(dest ...any) error
}

func (m *mockRow) Scan(dest ...any) error {
	if m.scanFunc != nil {
		return m.scanFunc(dest...)
	}
	return nil
}

// --- Тесты функциональных опций ---

func TestClientOptions(t *testing.T) {
	t.Parallel()

	t.Run("WithMaxParallelQueries sets concurrency", func(t *testing.T) {
		t.Parallel()
		c := &Client{}
		WithConcurrency(8)(c)
		require.Equal(t, 8, c.concurrency)

		// отрицательное значение игнорируется
		WithConcurrency(-1)(c)
		require.Equal(t, 8, c.concurrency)
	})

	t.Run("WithBatchTx enables/disables batch tx", func(t *testing.T) {
		t.Parallel()
		c := &Client{}

		WithBatchTx(true)(c)
		require.True(t, c.batchTx)

		WithBatchTx(false)(c)
		require.False(t, c.batchTx)
	})

	t.Run("WithSchemaReplacer sets custom replacer", func(t *testing.T) {
		t.Parallel()
		c := &Client{}
		called := false
		fn := func(sql, schema string) string {
			called = true
			return sql + ":" + schema
		}

		WithSchemaReplacer(fn)(c)
		require.NotNil(t, c.schemaReplacer)

		result := c.schemaReplacer("SELECT * FROM {schema}", "bucket_0")
		require.True(t, called)
		require.Equal(t, "SELECT * FROM {schema}:bucket_0", result)
	})

	t.Run("WithPoolOptions appends options", func(t *testing.T) {
		t.Parallel()
		c := &Client{poolOpts: []PoolOption{}}

		opt1 := func(_ *pgxpool.Config) {}
		opt2 := func(_ *pgxpool.Config) {}

		WithPoolOptions(opt1)(c)
		WithPoolOptions(opt2)(c)

		require.Len(t, c.poolOpts, 2)
	})
}

// --- Тесты resolve ---

func TestClient_resolve(t *testing.T) {
	t.Parallel()

	t.Run("success", func(t *testing.T) {
		t.Parallel()
		cm := clientMocks(t)

		cm.mappingMock.EXPECT().
			GetShard(BucketID(2)).
			Return("shard_1", nil)

		shard, schema, err := cm.resolve(BucketID(2))
		require.NoError(t, err)
		require.Equal(t, "shard_1", shard)
		require.Equal(t, "bucket_2", schema)
	})

	t.Run("mapping error", func(t *testing.T) {
		t.Parallel()
		cm := clientMocks(t)

		_, _, err := cm.resolve(BucketID(5))
		require.Error(t, err)
	})
}

// --- Тесты replaceSchema ---

func TestClient_replaceSchema(t *testing.T) {
	t.Parallel()

	t.Run("default replacer", func(t *testing.T) {
		t.Parallel()
		c := &Client{}

		got := c.replaceSchema(
			`INSERT INTO {schema}.users (id) VALUES ($1)`,
			"bucket_0",
		)
		require.Equal(t, `INSERT INTO bucket_0.users (id) VALUES ($1)`, got)
	})

	t.Run("default replacer handles multiple placeholders", func(t *testing.T) {
		t.Parallel()
		c := &Client{}

		got := c.replaceSchema(
			`SELECT * FROM {schema}.users JOIN {schema}.orders ON ...`,
			"bucket_1",
		)
		require.Equal(t, `SELECT * FROM bucket_1.users JOIN bucket_1.orders ON ...`, got)
	})

	t.Run("custom replacer", func(t *testing.T) {
		t.Parallel()
		c := &Client{
			schemaReplacer: func(sql, schema string) string {
				return "custom:" + schema
			},
		}

		got := c.replaceSchema(`SELECT 1`, "bucket_2")
		require.Equal(t, "custom:bucket_2", got)
	})

	t.Run("no placeholder returns same string", func(t *testing.T) {
		t.Parallel()
		c := &Client{}

		got := c.replaceSchema(`SELECT 1`, "bucket_0")
		require.Equal(t, `SELECT 1`, got)
	})
}

// --- Тесты getPoolByBucket ---

func TestClient_getPoolByBucket(t *testing.T) {
	t.Parallel()

	t.Run("success", func(t *testing.T) {
		t.Parallel()
		cm := clientMocks(t)

		cm.mappingMock.EXPECT().
			GetShard(BucketID(0)).
			Return("shard_1", nil)
		cm.wrapPoolMock.EXPECT().
			GetPool("shard_1").
			Return(cm.poolMock, nil)

		pool, schema, err := cm.getPoolByBucket(BucketID(0))
		require.NoError(t, err)
		require.Same(t, cm.poolMock, pool)
		require.Equal(t, "bucket_0", schema)
	})

	t.Run("mapping error", func(t *testing.T) {
		t.Parallel()
		cm := clientMocks(t)

		cm.mappingMock.EXPECT().
			GetShard(BucketID(0)).
			Return("", assert.AnError)

		pool, _, err := cm.getPoolByBucket(BucketID(0))
		require.ErrorIs(t, err, assert.AnError)
		require.Nil(t, pool)
	})

	t.Run("pool not found", func(t *testing.T) {
		t.Parallel()
		cm := clientMocks(t)

		cm.mappingMock.EXPECT().
			GetShard(BucketID(0)).
			Return("shard_1", nil)
		cm.wrapPoolMock.EXPECT().
			GetPool("shard_1").
			Return(nil, assert.AnError)

		pool, _, err := cm.getPoolByBucket(BucketID(0))
		require.ErrorIs(t, err, assert.AnError)
		require.Nil(t, pool)
	})
}

// --- Тесты Close ---

func TestClient_Close(t *testing.T) {
	t.Parallel()

	t.Run("closes wrapPool", func(t *testing.T) {
		t.Parallel()
		cm := clientMocks(t)

		cm.wrapPoolMock.EXPECT().
			Close().
			Once()

		cm.Close()
	})
}
