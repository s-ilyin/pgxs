package pgxs

import (
	"testing"

	"github.com/jackc/pgx/v5/pgconn"
	"github.com/pashagolub/pgxmock/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
)

// testPrehasher — реализация PreHasher для тестов
type testPrehasher string

func (t testPrehasher) PreHash() []byte { return []byte(t) }

func TestClient_ExecBucket(t *testing.T) {
	t.Parallel()

	t.Run("success", func(t *testing.T) {
		t.Parallel()

		cm := clientMocks(t)
		cm.mappingMock.EXPECT().
			GetShard(BucketID(0)).
			Return(mock.Anything, nil)
		cm.wrapPoolMock.EXPECT().
			GetPool(mock.Anything).
			Return(cm.poolMock, nil)
		cm.poolMock.EXPECT().
			Exec(mock.Anything, `INSERT INTO bucket_0.users (name) VALUES ($1)`, mock.Anything).
			Return(pgconn.NewCommandTag("INSERT 1"), nil)

		tag, err := cm.ExecBucket(t.Context(), 0, `INSERT INTO {schema}.users (name) VALUES ($1)`, "Alice")
		require.NoError(t, err)
		require.Equal(t, "INSERT 1", tag.String())
	})

	t.Run("pool not found wrong mapping", func(t *testing.T) {
		t.Parallel()

		cm := clientMocks(t)
		cm.mappingMock.EXPECT().
			GetShard(BucketID(0)).
			Return(mock.Anything, nil)
		cm.wrapPoolMock.EXPECT().
			GetPool(mock.Anything).
			Return(nil, assert.AnError)

		_, err := cm.ExecBucket(t.Context(), 0, `INSERT INTO {schema}.users (name) VALUES ($1)`, "Alice")
		require.Error(t, err)
		require.ErrorIs(t, err, assert.AnError)
	})

	t.Run("shard not found wrong mapping", func(t *testing.T) {
		t.Parallel()

		cm := clientMocks(t)
		cm.mappingMock.EXPECT().
			GetShard(BucketID(0)).
			Return("", assert.AnError)

		_, err := cm.ExecBucket(t.Context(), 0, `INSERT INTO {schema}.users (name) VALUES ($1)`, "Alice")
		require.Error(t, err)
		require.ErrorIs(t, err, assert.AnError)
	})

	t.Run("exec error", func(t *testing.T) {
		t.Parallel()

		cm := clientMocks(t)
		cm.mappingMock.EXPECT().
			GetShard(BucketID(0)).
			Return(mock.Anything, nil)
		cm.wrapPoolMock.EXPECT().
			GetPool(mock.Anything).
			Return(cm.poolMock, nil)
		cm.poolMock.EXPECT().
			Exec(mock.Anything, `INSERT INTO bucket_0.users (name) VALUES ($1)`, mock.Anything).
			Return(pgconn.CommandTag{}, assert.AnError)

		_, err := cm.ExecBucket(t.Context(), 0, `INSERT INTO {schema}.users (name) VALUES ($1)`, "Alice")
		require.Error(t, err)
		require.ErrorIs(t, err, assert.AnError)
	})
}

func TestClient_QueryBucket(t *testing.T) {
	t.Parallel()

	t.Run("success", func(t *testing.T) {
		t.Parallel()
		var (
			cm       = clientMocks(t)
			mockRows = pgxmock.NewRows([]string{"name"}).AddRow("Alice").Kind()
		)

		cm.mappingMock.EXPECT().
			GetShard(BucketID(1)).
			Return(mock.Anything, nil)
		cm.wrapPoolMock.EXPECT().
			GetPool(mock.Anything).
			Return(cm.poolMock, nil)
		cm.poolMock.EXPECT().
			Query(mock.Anything, `SELECT id FROM bucket_1.users WHERE name = $1`, mock.Anything).
			Return(mockRows, nil)

		rows, err := cm.QueryBucket(t.Context(), 1, `SELECT id FROM {schema}.users WHERE name = $1`, "Alice")
		require.NoError(t, err)
		require.NotNil(t, rows)
	})

	t.Run("pool not found wrong mapping", func(t *testing.T) {
		t.Parallel()

		cm := clientMocks(t)
		cm.mappingMock.EXPECT().
			GetShard(BucketID(1)).
			Return("", nil)
		cm.wrapPoolMock.EXPECT().
			GetPool(mock.Anything).
			Return(nil, assert.AnError)

		_, err := cm.QueryBucket(t.Context(), 1, `SELECT id FROM {schema}.users WHERE name = $1`, "Alice")
		require.Error(t, err)
		require.ErrorIs(t, err, assert.AnError)
	})

	t.Run("shard not found wrong mapping", func(t *testing.T) {
		t.Parallel()

		cm := clientMocks(t)
		cm.mappingMock.EXPECT().
			GetShard(BucketID(1)).
			Return("", assert.AnError)

		_, err := cm.QueryBucket(t.Context(), 1, `SELECT id FROM {schema}.users WHERE name = $1`, "Alice")
		require.Error(t, err)
		require.ErrorIs(t, err, assert.AnError)
	})

	t.Run("query error", func(t *testing.T) {
		t.Parallel()

		cm := clientMocks(t)
		cm.mappingMock.EXPECT().
			GetShard(BucketID(1)).
			Return(mock.Anything, nil)
		cm.wrapPoolMock.EXPECT().
			GetPool(mock.Anything).
			Return(cm.poolMock, nil)
		cm.poolMock.EXPECT().
			Query(mock.Anything, `SELECT id FROM bucket_1.users WHERE name = $1`, mock.Anything).
			Return(nil, assert.AnError)

		_, err := cm.QueryBucket(t.Context(), 1, `SELECT id FROM {schema}.users WHERE name = $1`, "Alice")
		require.Error(t, err)
		require.ErrorIs(t, err, assert.AnError)
	})
}

func TestClient_QueryRowBucket(t *testing.T) {
	t.Parallel()

	t.Run("success", func(t *testing.T) {
		t.Parallel()

		cm := clientMocks(t)
		mockRow := &mockRow{
			scanFunc: func(dest ...any) error {
				if len(dest) > 0 {
					if ptr, ok := dest[0].(*string); ok {
						*ptr = "1"
					}
				}
				return nil
			},
		}

		cm.mappingMock.EXPECT().
			GetShard(BucketID(1)).
			Return(mock.Anything, nil)
		cm.wrapPoolMock.EXPECT().
			GetPool(mock.Anything).
			Return(cm.poolMock, nil)
		cm.poolMock.EXPECT().
			QueryRow(mock.Anything, `SELECT id FROM bucket_1.users WHERE name = $1`, mock.Anything).
			Return(mockRow)

		row := cm.QueryRowBucket(t.Context(), 1, `SELECT id FROM {schema}.users WHERE name = $1`, "Alice")
		require.NotNil(t, row)

		var id string
		err := row.Scan(&id)
		require.NoError(t, err)
		require.Equal(t, "1", id)
	})

	t.Run("pool not found wrong mapping", func(t *testing.T) {
		t.Parallel()

		cm := clientMocks(t)
		cm.mappingMock.EXPECT().
			GetShard(BucketID(1)).
			Return("", nil)
		cm.wrapPoolMock.EXPECT().
			GetPool(mock.Anything).
			Return(nil, assert.AnError)

		row := cm.QueryRowBucket(t.Context(), 1, `SELECT id FROM {schema}.users WHERE name = $1`, "Alice")
		require.NotNil(t, row)

		var id string
		err := row.Scan(&id)
		require.Error(t, err)
		require.ErrorIs(t, err, assert.AnError)
	})

	t.Run("shard not found wrong mapping", func(t *testing.T) {
		t.Parallel()

		cm := clientMocks(t)
		cm.mappingMock.EXPECT().
			GetShard(BucketID(1)).
			Return("", assert.AnError)

		row := cm.QueryRowBucket(t.Context(), 1, `SELECT id FROM {schema}.users WHERE name = $1`, "Alice")
		require.NotNil(t, row)

		var id string
		err := row.Scan(&id)
		require.Error(t, err)
		require.ErrorIs(t, err, assert.AnError)
	})
}

func TestClient_Exec(t *testing.T) {
	t.Parallel()

	t.Run("success", func(t *testing.T) {
		t.Parallel()
		cm := clientMocks(t)
		key := testPrehasher("user_key")

		cm.mappingMock.EXPECT().
			GetShard(mock.Anything).
			Return(mock.Anything, nil)
		cm.wrapPoolMock.EXPECT().
			GetPool(mock.Anything).
			Return(cm.poolMock, nil)
		cm.poolMock.EXPECT().
			// BucketID == 2 вычисляется хеш-функцией. Поэтому подставляем сразу bucket_2.
			// Можно в тесте вычислить, но избыточно.
			Exec(mock.Anything, `INSERT INTO bucket_2.users (name) VALUES ($1)`, mock.Anything).
			Return(pgconn.NewCommandTag("INSERT 1"), nil)

		tag, err := cm.ExecPresher(t.Context(), key, `INSERT INTO {schema}.users (name) VALUES ($1)`, "Alice")
		require.NoError(t, err)
		require.Equal(t, "INSERT 1", tag.String())
	})

	t.Run("pool not found wrong mapping", func(t *testing.T) {
		t.Parallel()

		cm := clientMocks(t)
		key := testPrehasher("user_key")

		cm.mappingMock.EXPECT().
			GetShard(mock.Anything).
			Return("", nil)
		cm.wrapPoolMock.EXPECT().
			GetPool(mock.Anything).
			Return(nil, assert.AnError)

		_, err := cm.ExecPresher(t.Context(), key, `INSERT INTO {schema}.users (name) VALUES ($1)`, "Alice")
		require.Error(t, err)
		require.ErrorIs(t, err, assert.AnError)
	})

	t.Run("shard not found wrong mapping", func(t *testing.T) {
		t.Parallel()

		cm := clientMocks(t)
		key := testPrehasher("user_key")

		cm.mappingMock.EXPECT().
			GetShard(mock.Anything).
			Return("", assert.AnError)

		_, err := cm.ExecPresher(t.Context(), key, `INSERT INTO {schema}.users (name) VALUES ($1)`, "Alice")
		require.Error(t, err)
		require.ErrorIs(t, err, assert.AnError)
	})

	t.Run("exec error", func(t *testing.T) {
		t.Parallel()

		cm := clientMocks(t)
		key := testPrehasher("user_key")

		cm.mappingMock.EXPECT().
			GetShard(mock.Anything).
			Return(mock.Anything, nil)
		cm.wrapPoolMock.EXPECT().
			GetPool(mock.Anything).
			Return(cm.poolMock, nil)
		cm.poolMock.EXPECT().
			// BucketID == 2 вычисляется хеш-функцией. Поэтому подставляем сразу bucket_2.
			// Можно в тесте вычислить, но избыточно.
			Exec(mock.Anything, `INSERT INTO bucket_2.users (name) VALUES ($1)`, mock.Anything).
			Return(pgconn.CommandTag{}, assert.AnError)

		_, err := cm.ExecPresher(t.Context(), key, `INSERT INTO {schema}.users (name) VALUES ($1)`, "Alice")
		require.Error(t, err)
		require.ErrorIs(t, err, assert.AnError)
	})
}

func TestClient_Query(t *testing.T) {
	t.Parallel()

	t.Run("success", func(t *testing.T) {
		t.Parallel()
		cm := clientMocks(t)
		key := testPrehasher("user_key")
		mockRows := pgxmock.NewRows([]string{"id"}).AddRow("1").Kind()

		cm.mappingMock.EXPECT().
			GetShard(mock.Anything).
			Return(mock.Anything, nil)
		cm.wrapPoolMock.EXPECT().
			GetPool(mock.Anything).
			Return(cm.poolMock, nil)
		cm.poolMock.EXPECT().
			Query(mock.Anything, `SELECT id FROM bucket_2.users WHERE name = $1`, mock.Anything).
			Return(mockRows, nil)

		rows, err := cm.QueryPresher(t.Context(), key, `SELECT id FROM {schema}.users WHERE name = $1`, "Alice")
		require.NoError(t, err)
		require.NotNil(t, rows)
	})

	t.Run("pool not found wrong mapping", func(t *testing.T) {
		t.Parallel()

		cm := clientMocks(t)
		key := testPrehasher("user_key")

		cm.mappingMock.EXPECT().
			GetShard(mock.Anything).
			Return("", nil)
		cm.wrapPoolMock.EXPECT().
			GetPool(mock.Anything).
			Return(nil, assert.AnError)

		_, err := cm.QueryPresher(t.Context(), key, `SELECT id FROM {schema}.users WHERE name = $1`, "Alice")
		require.Error(t, err)
		require.ErrorIs(t, err, assert.AnError)
	})

	t.Run("shard not found wrong mapping", func(t *testing.T) {
		t.Parallel()

		cm := clientMocks(t)
		key := testPrehasher("user_key")

		cm.mappingMock.EXPECT().
			GetShard(mock.Anything).
			Return("", assert.AnError)

		_, err := cm.QueryPresher(t.Context(), key, `SELECT id FROM {schema}.users WHERE name = $1`, "Alice")
		require.Error(t, err)
		require.ErrorIs(t, err, assert.AnError)
	})

	t.Run("query error", func(t *testing.T) {
		t.Parallel()

		cm := clientMocks(t)
		key := testPrehasher("user_key")

		cm.mappingMock.EXPECT().
			GetShard(mock.Anything).
			Return(mock.Anything, nil)
		cm.wrapPoolMock.EXPECT().
			GetPool(mock.Anything).
			Return(cm.poolMock, nil)
		cm.poolMock.EXPECT().
			Query(mock.Anything, `SELECT id FROM bucket_2.users WHERE name = $1`, mock.Anything).
			Return(nil, assert.AnError)

		_, err := cm.QueryPresher(t.Context(), key, `SELECT id FROM {schema}.users WHERE name = $1`, "Alice")
		require.Error(t, err)
		require.ErrorIs(t, err, assert.AnError)
	})
}

func TestClient_QueryRow(t *testing.T) {
	t.Parallel()

	t.Run("success", func(t *testing.T) {
		t.Parallel()

		var (
			cm      = clientMocks(t)
			key     = testPrehasher("user_key")
			mockRow = &mockRow{
				scanFunc: func(dest ...any) error {
					if len(dest) > 0 {
						if ptr, ok := dest[0].(*string); ok {
							*ptr = "1"
						}
					}
					return nil
				},
			}
		)

		cm.mappingMock.EXPECT().
			GetShard(mock.Anything).
			Return(mock.Anything, nil)
		cm.wrapPoolMock.EXPECT().
			GetPool(mock.Anything).
			Return(cm.poolMock, nil)
		cm.poolMock.EXPECT().
			QueryRow(mock.Anything, `SELECT id FROM bucket_2.users WHERE name = $1`, mock.Anything).
			Return(mockRow)

		row := cm.QueryRowPresher(t.Context(), key, `SELECT id FROM {schema}.users WHERE name = $1`, "Alice")
		require.NotNil(t, row)

		var id string
		err := row.Scan(&id)
		require.NoError(t, err)
		require.Equal(t, "1", id)
	})

	t.Run("pool not found wrong mapping", func(t *testing.T) {
		t.Parallel()

		cm := clientMocks(t)
		key := testPrehasher("user_key")

		cm.mappingMock.EXPECT().
			GetShard(mock.Anything).
			Return("", nil)
		cm.wrapPoolMock.EXPECT().
			GetPool(mock.Anything).
			Return(nil, assert.AnError)

		row := cm.QueryRowPresher(t.Context(), key, `SELECT id FROM {schema}.users WHERE name = $1`, "Alice")
		require.NotNil(t, row)

		var id string
		err := row.Scan(&id)
		require.Error(t, err)
		require.ErrorIs(t, err, assert.AnError)
	})

	t.Run("shard not found wrong mapping", func(t *testing.T) {
		t.Parallel()

		cm := clientMocks(t)
		key := testPrehasher("user_key")

		cm.mappingMock.EXPECT().
			GetShard(mock.Anything).
			Return("", assert.AnError)

		row := cm.QueryRowPresher(t.Context(), key, `SELECT id FROM {schema}.users WHERE name = $1`, "Alice")
		require.NotNil(t, row)

		var id string
		err := row.Scan(&id)
		require.Error(t, err)
		require.ErrorIs(t, err, assert.AnError)
	})
}
