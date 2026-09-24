package pgxs

import (
	"context"

	"github.com/jackc/pgx/v5"
)

func (c *Client) BeginTx(ctx context.Context, txOptions pgx.TxOptions) (pgx.Tx, error) {
	bucketID, err := c.bucketFromContext(ctx)
	if err != nil {
		return nil, err
	}

	return c.BeginTxBucket(ctx, bucketID, txOptions)
}

func (c *Client) Begin(ctx context.Context) (pgx.Tx, error) {
	bucketID, err := c.bucketFromContext(ctx)
	if err != nil {
		return nil, err
	}

	return c.BeginBucket(ctx, bucketID)
}

func (c *Client) BeginTxPreHasher(ctx context.Context, key Prehasher, txOptions pgx.TxOptions) (pgx.Tx, error) {
	bucketID := BucketID(HashKey(key.Prehash(), c.config.Buckets))
	return c.BeginTxBucket(ctx, bucketID, txOptions)
}

func (c *Client) BeginPreHasher(ctx context.Context, key Prehasher) (pgx.Tx, error) {
	bucketID := BucketID(HashKey(key.Prehash(), c.config.Buckets))
	return c.BeginBucket(ctx, bucketID)
}

func (c *Client) BeginBucket(ctx context.Context, bucketID BucketID) (pgx.Tx, error) {
	pool, schema, err := c.getPoolByBucket(bucketID)
	if err != nil {
		return nil, err
	}
	tx, err := pool.Begin(ctx)
	if err != nil {
		return nil, err
	}
	return &wrapTx{tx: tx, schema: schema, client: c}, nil
}

func (c *Client) BeginTxBucket(ctx context.Context, bucketID BucketID, txOptions pgx.TxOptions) (pgx.Tx, error) {
	pool, schema, err := c.getPoolByBucket(bucketID)
	if err != nil {
		return nil, err
	}
	tx, err := pool.BeginTx(ctx, txOptions)
	if err != nil {
		return nil, err
	}
	return &wrapTx{tx: tx, schema: schema, client: c}, nil
}
