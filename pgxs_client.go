package pgxs

import (
	"context"
	"strconv"
	"strings"
)

// Client – основной маршрутизатор запросов по шардам.
type Client struct {
	config         *Config
	mapping        *mapping
	wrapPool       *wrapPool
	maxParallel    int
	batchTx        bool
	schemaReplacer func(string, string) string
	poolOpts       []PoolOption
}

// ClientOption – функциональная опция для клиента.
type ClientOption func(*Client)

// WithMaxParallelQueries устанавливает максимальное число одновременных запросов в QueryAll.
func WithMaxParallelQueries(n int) ClientOption {
	return func(c *Client) {
		if n > 0 {
			c.maxParallel = n
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

// NewClient создаёт новый клиент с заданным конфигом и опциями.
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
		maxParallel: 32,
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

	return c, nil
}

// Close закрывает все пулы.
func (c *Client) Close() {
	c.wrapPool.Close()
}

// ---- Вспомогательные методы ----

func (c *Client) resolve(bucketID BucketID) (shardName, schemaName string, err error) {
	shard, err := c.mapping.GetShard(bucketID)
	if err != nil {
		return "", "", err
	}
	schema := c.config.SchemaPrefix + strconv.Itoa(int(bucketID))
	return shard, schema, nil
}

func (c *Client) replaceSchema(sql, schema string) string {
	if c.schemaReplacer != nil {
		return c.schemaReplacer(sql, schema)
	}
	return strings.ReplaceAll(sql, "{schema}", schema)
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
