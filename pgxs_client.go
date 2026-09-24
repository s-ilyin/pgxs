package pgxs

import (
	"context"
	"fmt"
	"runtime"
	"strconv"
	"strings"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
)

type RetryPool interface {
	Exec(ctx context.Context, sql string, args ...any) (pgconn.CommandTag, error)
	Query(ctx context.Context, sql string, args ...any) (pgx.Rows, error)
	QueryRow(ctx context.Context, sql string, args ...any) pgx.Row
	SendBatch(ctx context.Context, batch *pgx.Batch) pgx.BatchResults
	Begin(ctx context.Context) (pgx.Tx, error)
	BeginTx(ctx context.Context, txOptions pgx.TxOptions) (pgx.Tx, error)
	Stat() *pgxpool.Stat
	Ping(ctx context.Context) error
	Acquire(ctx context.Context) (RetryConn, error)
	Close()
}

// RetryConner — интерфейс для RetryConn.
type RetryConn interface {
	Exec(ctx context.Context, sql string, args ...any) (pgconn.CommandTag, error)
	Query(ctx context.Context, sql string, args ...any) (pgx.Rows, error)
	QueryRow(ctx context.Context, sql string, args ...any) pgx.Row
	SendBatch(ctx context.Context, batch *pgx.Batch) pgx.BatchResults
	Begin(ctx context.Context) (pgx.Tx, error)
	BeginTx(ctx context.Context, txOptions pgx.TxOptions) (pgx.Tx, error)
	Release()
}

// Client – основной маршрутизатор запросов по шардам.
type Client struct {
	config         *Config
	mapping        Mapping
	wrapPool       WrapPool
	schemaReplacer func(string, string) string
	poolOpts       []PoolOption
	concurrency    int
	batchTx        bool
	schemaNames    []string
}

// ClientOption – функциональная опция для клиента.
type ClientOption func(*Client)

// WithMaxParallelQueries устанавливает максимальное число одновременных запросов при параллельным запросах.
func WithConcurrency(n int) ClientOption {
	return func(c *Client) {
		if n > 0 {
			c.concurrency = n
		}
	}
}

// WithBatchTx включает выполнение батча в транзакции на каждом шарде.
func WithBatchTx(enabled bool) ClientOption {
	return func(c *Client) {
		c.batchTx = enabled
	}
}

// WithSchemaReplacer позволяет задать кастомную функцию замены {schema} в SQL.
func WithSchemaReplacer(fn func(sql, schema string) string) ClientOption {
	return func(c *Client) {
		c.schemaReplacer = fn
	}
}

// WithPoolOptions передаёт настройки пула в клиент.
func WithPoolOptions(opts ...PoolOption) ClientOption {
	return func(c *Client) {
		c.poolOpts = append(c.poolOpts, opts...)
	}
}

func DefaultConcurrency(shards int) int {
	gmp := runtime.GOMAXPROCS(0) // учитывает cgroup limits
	want := min(max(gmp*2, 4), 8)
	if shards < want {
		return shards
	}
	return want
}

// New создаёт новый клиент с заданным конфигом и опциями.
func New(ctx context.Context, cfg *Config, opts ...ClientOption) (*Client, error) {
	if err := cfg.Validate(); err != nil {
		return nil, err
	}
	mapping, err := newMapping(cfg)
	if err != nil {
		return nil, err
	}

	if cfg.Retry.MaxAttempts == 0 {
		cfg.Retry.MaxAttempts = 1
		cfg.Retry.BaseDelay = 0
		cfg.Retry.MaxDelay = 0
	}

	c := &Client{
		config:      cfg,
		mapping:     mapping,
		concurrency: DefaultConcurrency(len(cfg.Shards)),
		batchTx:     true,
		poolOpts:    []PoolOption{},
	}
	for _, opt := range opts {
		opt(c)
	}
	wrapPool, err := newPool(ctx, cfg, c.poolOpts...)
	if err != nil {
		return nil, err
	}
	c.wrapPool = wrapPool

	c.schemaNames = make([]string, cfg.Buckets)
	for i := range int(cfg.Buckets) {
		c.schemaNames[i] = cfg.SchemaPrefix + strconv.Itoa(i)
	}

	return c, nil
}

// Close закрывает все пулы.
func (c *Client) Close() {
	c.wrapPool.Close()
}

// ---- Вспомогательные методы ----

func (c *Client) resolve(bucketID BucketID) (shardName, schemaName string, err error) {
	err = bucketID.Validate(c.config.Buckets)
	if err != nil {
		return "", "", fmt.Errorf("resolve bucket: %w", err)
	}

	shard, err := c.mapping.GetShard(bucketID)
	if err != nil {
		return "", "", err
	}
	return shard, c.schemaNames[bucketID], nil
}

func (c *Client) replaceSchema(sql, schema string) string {
	if c.schemaReplacer != nil {
		return c.schemaReplacer(sql, schema)
	}
	return strings.ReplaceAll(sql, BucketPattern, schema)
}

func (c *Client) getPoolByBucket(bucketID BucketID) (RetryPool, string, error) {
	shard, schema, err := c.resolve(bucketID)
	if err != nil {
		return nil, "", err
	}
	pool, err := c.wrapPool.GetPool(shard)
	if err != nil {
		return nil, "", err
	}
	return pool, schema, nil
}
