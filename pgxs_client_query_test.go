package pgxs

import (
	"errors"
	"testing"

	"github.com/jackc/pgx/v5/pgconn"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
)

// testPrehasher — реализация PreHasher для тестов
type testPrehasher string

func (t testPrehasher) PreHash() []byte { return []byte(t) }

func TestClient_ExecBucket(t *testing.T) {
	clientMock := clientMocks(t)

	t.Run("success", func(t *testing.T) {
		clientMock.poolMock.EXPECT().
			Exec(mock.Anything, `INSERT INTO bucket_0.users (name) VALUES ($1)`, "Alice").
			Return(pgconn.NewCommandTag("INSERT 1"), nil)

		tag, err := clientMock.ExecBucket(t.Context(), 0, `INSERT INTO {schema}.users (name) VALUES ($1)`, "Alice")
		require.NoError(t, err)
		require.Equal(t, "INSERT 1", tag.String())
	})

	t.Run("pool not found (wrong mapping)", func(t *testing.T) {
		cfg := &Config{
			Buckets:      4,
			SchemaPrefix: "bucket_",
			Shards: []Shard{
				{Name: "shard1", DSN: "postgres://localhost:5432/db"},
			},
			Mapping: []MappingEntry{
				{Bucket: 0, Shard: "shard2"}, // несуществующий шард
			},
		}
		client, err := New(t.Context(), cfg)
		require.NoError(t, err)

		_, err = client.ExecBucket(t.Context(), 0, `INSERT INTO {schema}.users (name) VALUES ($1)`, "Alice")
		require.Error(t, err)
		require.Contains(t, err.Error(), "shard")
	})

	t.Run("exec error", func(t *testing.T) {
		clientMock.poolMock.EXPECT().
			Exec(mock.Anything, `INSERT INTO bucket_0.users (name) VALUES ($1)`, "Alice").
			Return(pgconn.CommandTag{}, assert.AnError)

		_, err := clientMock.ExecBucket(t.Context(), 0, `INSERT INTO {schema}.users (name) VALUES ($1)`, "Alice")
		require.Error(t, err)
	})
}

func TestClient_QueryBucket(t *testing.T) {
	clientMock := clientMocks(t)
	// rows := pgxmock.NewRows(nil).AddRows(nil)
	t.Run("success", func(t *testing.T) {
		clientMock.poolMock.EXPECT().
			Query(mock.Anything, `SELECT id FROM bucket_0.users WHERE name = $1`, "Alice").
			Return(nil, nil)

		rows, err := clientMock.QueryBucket(t.Context(), 0, `SELECT id FROM {schema}.users WHERE name = $1`, "Alice")
		require.NoError(t, err)
		require.NotNil(t, rows)
	})

	t.Run("query error", func(t *testing.T) {
		expectedErr := errors.New("query failed")
		clientMock.poolMock.EXPECT().
			Query(mock.Anything, `SELECT id FROM bucket_0.users WHERE name = $1`, "Alice").
			Return(nil, expectedErr)

		_, err := clientMock.QueryBucket(t.Context(), 0, `SELECT id FROM {schema}.users WHERE name = $1`, "Alice")
		require.Error(t, err)
		require.Equal(t, expectedErr, err)
	})
}

// Аналогично для QueryRowBucket, Exec, Query, QueryRow с PreHasher
// (пишутся по тому же шаблону, используя cm.poolMock)
