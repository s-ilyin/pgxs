package pgxs

import (
	"context"

	"github.com/jackc/pgx/v5"
)

func (c *Client) BeginTx(ctx context.Context, key PreHasher, txOptions pgx.TxOptions) (Tx, error) {
	bucketID := BucketID(HashKey(key.PreHash(), c.config.Buckets))
	return c.BeginTxBucket(ctx, bucketID, txOptions)
}

func (c *Client) Begin(ctx context.Context, key PreHasher) (Tx, error) {
	bucketID := BucketID(HashKey(key.PreHash(), c.config.Buckets))
	return c.BeginBucket(ctx, bucketID)
}

func (c *Client) BeginBucket(ctx context.Context, bucketID BucketID) (Tx, error) {
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

func (c *Client) BeginTxBucket(ctx context.Context, bucketID BucketID, txOptions pgx.TxOptions) (Tx, error) {
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
