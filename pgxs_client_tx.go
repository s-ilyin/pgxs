package pgxs

import (
	"context"
)

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
