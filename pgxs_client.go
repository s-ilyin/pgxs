package pgxs

import (
	"context"
	"strconv"
	"strings"

	"github.com/jackc/pgx/v5/pgxpool"
)

// Client – основной маршрутизатор запросов по шардам.
type Client struct {
	config         *Config
	mapping        *mapping
	wrapPool       *wrapPool
	maxParallel    int
	batchTx        bool
	schemaReplacer func(string, string) string
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

// NewClient создаёт новый клиент с заданным конфигом и опциями.
func New(ctx context.Context, cfg *Config, opts ...ClientOption) (*Client, error) {
	if err := cfg.Validate(); err != nil {
		return nil, err
	}
	mapping, err := newMapping(cfg)
	if err != nil {
		return nil, err
	}
	pools, err := newPool(ctx, cfg)
	if err != nil {
		return nil, err
	}

	c := &Client{
		config:      cfg,
		mapping:     mapping,
		wrapPool:    pools,
		maxParallel: 32,
		batchTx:     true,
	}
	for _, opt := range opts {
		opt(c)
	}
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

func (c *Client) getPoolByBucket(bucketID BucketID) (*pgxpool.Pool, string, error) {
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
