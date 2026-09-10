package pgxs

import (
	"context"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
)

func (c *Client) ExecBucket(ctx context.Context, bucketID BucketID, sql string, args ...any) (pgconn.CommandTag, error) {
	pool, schema, err := c.getPoolByBucket(bucketID)
	if err != nil {
		return pgconn.CommandTag{}, err
	}
	sql = c.replaceSchema(sql, schema)
	return pool.Exec(ctx, sql, args...)
}

func (c *Client) QueryBucket(ctx context.Context, bucketID BucketID, sql string, args ...any) (pgx.Rows, error) {
	pool, schema, err := c.getPoolByBucket(bucketID)
	if err != nil {
		return nil, err
	}
	sql = c.replaceSchema(sql, schema)
	return pool.Query(ctx, sql, args...)
}

func (c *Client) QueryRowBucket(ctx context.Context, bucketID BucketID, sql string, args ...any) pgx.Row {
	pool, schema, err := c.getPoolByBucket(bucketID)
	if err != nil {
		return &errorRow{err: err}
	}
	sql = c.replaceSchema(sql, schema)
	return pool.QueryRow(ctx, sql, args...)
}

// ---- Одиночные операции с ShardKey ----
func (c *Client) Query(ctx context.Context, key PreHasher, sql string, args ...any) (pgx.Rows, error) {
	bucketID := BucketID(HashKey(key.PreHash(), c.config.Buckets))
	return c.QueryBucket(ctx, bucketID, sql, args...)
}

func (c *Client) QueryRow(ctx context.Context, key PreHasher, sql string, args ...any) pgx.Row {
	bucketID := BucketID(HashKey(key.PreHash(), c.config.Buckets))
	return c.QueryRowBucket(ctx, bucketID, sql, args...)
}

func (c *Client) Exec(ctx context.Context, key PreHasher, sql string, args ...any) (pgconn.CommandTag, error) {
	bucketID := BucketID(HashKey(key.PreHash(), c.config.Buckets))
	return c.ExecBucket(ctx, bucketID, sql, args...)
}
