package pgxs

import (
	"context"
	"fmt"

	"github.com/jackc/pgx/v5"
)

type (
	ctxPreHasherKey string
	ctxBucketIDKey  string
	ctxTxKey        string
)

func ContextWithTx(ctx context.Context, tx pgx.Tx) context.Context {
	return context.WithValue(ctx, ctxTxKey("key_tx"), tx)
}

func FromContextTx(ctx context.Context) (pgx.Tx, bool) {
	v, ok := ctx.Value(ctxTxKey("key_tx")).(pgx.Tx)
	return v, ok
}

func ContextWithPrehasher(ctx context.Context, val Prehasher) context.Context {
	return context.WithValue(ctx, ctxPreHasherKey("key_prehasher_id"), val)
}

func FromContextPrehasher(ctx context.Context) (Prehasher, bool) {
	v, ok := ctx.Value(ctxPreHasherKey("key_prehasher_id")).(Prehasher)
	return v, ok
}

func ContextWithBucketID(ctx context.Context, val BucketID) context.Context {
	return context.WithValue(ctx, ctxBucketIDKey("key_bucket_id"), val)
}

func FromContextBucketID(ctx context.Context) (BucketID, bool) {
	v, ok := ctx.Value(ctxBucketIDKey("key_bucket_id")).(BucketID)
	return v, ok
}

func (c *Client) bucketFromContext(ctx context.Context) (BucketID, error) {
	bucketID, ok := FromContextBucketID(ctx)
	if ok {
		if err := bucketID.Validate(c.config.Buckets); err != nil {
			return 0, fmt.Errorf("bucket from context: %w", err)
		}

		return bucketID, nil
	}

	hasher, ok := FromContextPrehasher(ctx)
	if !ok {
		return 0, fmt.Errorf("no BucketID or PreHasher in context")
	}

	return BucketIdFromHash(hasher.Prehash(), c.config.Buckets), nil
}
