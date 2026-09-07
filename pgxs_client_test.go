package pgxs

import (
	"context"
	"testing"

	"github.com/pashagolub/pgxmock/v4"
	"github.com/s-ilyin/pgxs/mocks"
	"github.com/stretchr/testify/require"
)

type clientMock struct {
	*Client
	poolMock *mocks.RetryPool
	connMock *mocks.RetryConn
	pgxRows  pgxmock.Rows

	txMock      *mocks.Tx
	prehashMock *mocks.PreHasher
}

func clientMocks(t *testing.T) *clientMock {
	cfg := &Config{
		Buckets:      4,
		SchemaPrefix: "bucket_",
		Shards: []Shard{
			{Name: "shard1", DSN: "postgres://localhost:5432/db"},
		},
		Mapping: []MappingEntry{
			{Bucket: 0, Shard: "shard1"},
			{Bucket: 1, Shard: "shard1"},
			{Bucket: 2, Shard: "shard1"},
			{Bucket: 3, Shard: "shard1"},
		},
	}
	var (
		retryPoolMock = mocks.NewRetryPool(t)
		retryConnMock = mocks.NewRetryConn(t)
		txMock        = mocks.NewTx(t)
		prehashMock   = mocks.NewPreHasher(t)
	)
	client, err := New(context.Background(), cfg)
	require.NoError(t, err)

	return &clientMock{
		Client:      client,
		poolMock:    retryPoolMock,
		connMock:    retryConnMock,
		txMock:      txMock,
		prehashMock: prehashMock,
	}
}
