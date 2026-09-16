package pgxs

import (
	"context"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
)

func (c *Client) Query(ctx context.Context, sql string, args ...any) (pgx.Rows, error) {
	bucketID, err := c.bucketFromContext(ctx)
	if err != nil {
		return nil, err
	}

	return c.QueryBucket(ctx, bucketID, sql, args...)
}

func (c *Client) QueryRow(ctx context.Context, sql string, args ...any) pgx.Row {
	bucketID, err := c.bucketFromContext(ctx)
	if err != nil {
		return &errorRow{err: err}
	}
	return c.QueryRowBucket(ctx, bucketID, sql, args...)
}

func (c *Client) Exec(ctx context.Context, sql string, args ...any) (pgconn.CommandTag, error) {
	bucketID, err := c.bucketFromContext(ctx)
	if err != nil {
		return pgconn.CommandTag{}, err
	}

	return c.ExecBucket(ctx, bucketID, sql, args...)
}

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

func (c *Client) ExecPresher(ctx context.Context, key PreHasher, sql string, args ...any) (pgconn.CommandTag, error) {
	bucketID := BucketID(HashKey(key.PreHash(), c.config.Buckets))
	return c.ExecBucket(ctx, bucketID, sql, args...)
}

func (c *Client) QueryPresher(ctx context.Context, key PreHasher, sql string, args ...any) (pgx.Rows, error) {
	bucketID := BucketID(HashKey(key.PreHash(), c.config.Buckets))
	return c.QueryBucket(ctx, bucketID, sql, args...)
}

func (c *Client) QueryRowPresher(ctx context.Context, key PreHasher, sql string, args ...any) pgx.Row {
	bucketID := BucketID(HashKey(key.PreHash(), c.config.Buckets))
	return c.QueryRowBucket(ctx, bucketID, sql, args...)
}

func (c *Client) SendBatch(ctx context.Context, b *pgx.Batch) pgx.BatchResults {
	bucketID, err := c.bucketFromContext(ctx)
	if err != nil {
		// Возвращаем ошибку через BatchResults, чтобы сохранить семантику pgx
		return &batchResultsWithError{err: err}
	}
	return c.SendBatchBucket(ctx, bucketID, b)
}

func (c *Client) SendBatchBucket(ctx context.Context, bucketID BucketID, b *pgx.Batch) pgx.BatchResults {
	pool, schema, err := c.getPoolByBucket(bucketID)
	if err != nil {
		return &batchResultsWithError{err: err}
	}

	for i := range b.QueuedQueries {
		b.QueuedQueries[i].SQL = c.replaceSchema(b.QueuedQueries[i].SQL, schema)
	}

	return pool.SendBatch(ctx, b)
}
