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

func ContextWithPreHasherKey(ctx context.Context, val PreHasher) context.Context {
	return context.WithValue(ctx, ctxPreHasherKey("key_prehasher_id"), val)
}

func FromContextPreHasherKey(ctx context.Context) (PreHasher, bool) {
	v, ok := ctx.Value(ctxPreHasherKey("key_prehasher_id")).(PreHasher)
	return v, ok
}

func ContextWithBucketIDKey(ctx context.Context, val BucketID) context.Context {
	return context.WithValue(ctx, ctxBucketIDKey("key_bucket_id"), val)
}

func FromContextBucketIDKey(ctx context.Context) (BucketID, bool) {
	v, ok := ctx.Value(ctxBucketIDKey("key_bucket_id")).(BucketID)
	return v, ok
}

func (c *Client) bucketFromContext(ctx context.Context) (BucketID, error) {
	bucketID, ok := FromContextBucketIDKey(ctx)
	if ok {
		return bucketID, nil
	}

	hasher, ok := FromContextPreHasherKey(ctx)
	if !ok {
		return 0, fmt.Errorf("no BucketID or PreHasher in context")
	}

	return BucketIdFromHash(hasher.PreHash(), c.config.Buckets), nil
}
