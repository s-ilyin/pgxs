package pgxs

import (
	"context"
	"testing"

	"github.com/jackc/pgx/v5/pgconn"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
)

func TestExec(t *testing.T) {
	t.Parallel()

	type testItem struct {
		ID   string
		Name string
	}

	var (
		prehasher    = func(i testItem) []byte { return []byte(i.ID) }
		queryBuilder = func(i testItem) (string, []any) {
			return `INSERT INTO {schema}.users (id, name) VALUES ($1, $2)`, []any{i.ID, i.Name}
		}
	)

	t.Run("success single shard with tx", func(t *testing.T) {
		t.Parallel()
		cm := clientMocks(t)
		cm.batchTx = true // включаем транзакцию

		items := []testItem{{ID: "1", Name: "Alice"}}

		// Ожидания
		cm.mappingMock.EXPECT().
			GetShard(BucketID(0)).
			Return("shard_1", nil)

		cm.wrapPoolMock.EXPECT().
			GetPool("shard_1").
			Return(cm.poolMock, nil)

		cm.poolMock.EXPECT().
			Acquire(mock.Anything).
			Return(cm.connMock, nil)

		cm.connMock.EXPECT().
			Begin(mock.Anything).
			Return(cm.pgxTxMock, nil)

		cm.pgxBatchResultMock.EXPECT().
			Exec().
			Return(pgconn.NewCommandTag("INSERT 1"), nil)
		cm.pgxBatchResultMock.EXPECT().
			Close().
			Return(nil)

		cm.pgxTxMock.EXPECT().
			SendBatch(mock.Anything, mock.Anything).
			Return(cm.pgxBatchResultMock)

		cm.pgxTxMock.EXPECT().
			Commit(mock.Anything).
			Return(nil)
		cm.pgxTxMock.EXPECT().
			Rollback(mock.Anything).
			Return(nil)

		cm.connMock.EXPECT().
			Release().
			Return()

		result, err := Exec(t.Context(), cm.Client, items, prehasher, queryBuilder)
		require.NoError(t, err)
		require.NotNil(t, result)
		require.Equal(t, 1, result.Total)
		require.Equal(t, int64(1), result.RowsAffected)
		require.Contains(t, result.BatchResults, "shard_1")
		require.Len(t, result.BatchResults["shard_1"].CommandTags, 1)
		require.NoError(t, result.BatchResults.Err())
	})

	t.Run("success single shard without tx", func(t *testing.T) {
		t.Parallel()
		cm := clientMocks(t)
		cm.batchTx = false

		items := []testItem{{ID: "1", Name: "Alice"}}

		cm.mappingMock.EXPECT().
			GetShard(BucketID(0)).
			Return("shard_1", nil)

		cm.wrapPoolMock.EXPECT().
			GetPool("shard_1").
			Return(cm.poolMock, nil)

		cm.poolMock.EXPECT().
			Acquire(mock.Anything).
			Return(cm.connMock, nil)

		// Нет ожиданий Begin, Commit, Rollback

		cm.pgxBatchResultMock.EXPECT().
			Exec().
			Return(pgconn.NewCommandTag("INSERT 1"), nil)
		cm.pgxBatchResultMock.EXPECT().
			Close().
			Return(nil)

		cm.connMock.EXPECT().
			SendBatch(mock.Anything, mock.Anything).
			Return(cm.pgxBatchResultMock)

		cm.connMock.EXPECT().
			Release().
			Return()

		result, err := Exec(t.Context(), cm.Client, items, prehasher, queryBuilder)
		require.NoError(t, err)
		require.NotNil(t, result)
		require.Equal(t, 1, result.Total)
		require.Equal(t, int64(1), result.RowsAffected)
		require.Contains(t, result.BatchResults, "shard_1")
		require.NoError(t, result.BatchResults.Err())
	})

	t.Run("success multiple shards", func(t *testing.T) {
		t.Parallel()
		cm := clientMocks(t)
		cm.batchTx = true

		items := []testItem{
			{ID: "1", Name: "Alice"},
			{ID: "2", Name: "Bob"},
		}

		// изначально поставили нужные бакеты. TODO: замокать?
		cm.mappingMock.EXPECT().
			GetShard(BucketID(0)).
			Return("shard_1", nil).Once()
		cm.mappingMock.EXPECT().
			GetShard(BucketID(3)).
			Return("shard_2", nil).Once()

		// Для shard_1
		cm.wrapPoolMock.EXPECT().
			GetPool("shard_1").
			Return(cm.poolMock, nil)
		cm.poolMock.EXPECT().
			Acquire(mock.Anything).
			Return(cm.connMock, nil)
		cm.connMock.EXPECT().
			Begin(mock.Anything).
			Return(cm.pgxTxMock, nil)
		cm.pgxBatchResultMock.EXPECT().
			Exec().
			Return(pgconn.NewCommandTag("INSERT 1"), nil)
		cm.pgxBatchResultMock.EXPECT().
			Close().
			Return(nil)
		cm.pgxTxMock.EXPECT().
			SendBatch(mock.Anything, mock.Anything).
			Return(cm.pgxBatchResultMock)
		cm.pgxTxMock.EXPECT().
			Commit(mock.Anything).
			Return(nil)
		cm.pgxTxMock.EXPECT().
			Rollback(mock.Anything).
			Return(nil)
		cm.connMock.EXPECT().
			Release().
			Return()

		// Для shard_2
		cm.wrapPoolMock.EXPECT().
			GetPool("shard_2").
			Return(cm.poolMock, nil)
		cm.poolMock.EXPECT().
			Acquire(mock.Anything).
			Return(cm.connMock, nil)
		cm.connMock.EXPECT().
			Begin(mock.Anything).
			Return(cm.pgxTxMock, nil)
		cm.pgxBatchResultMock.EXPECT().
			Exec().
			Return(pgconn.NewCommandTag("INSERT 1"), nil)
		cm.pgxBatchResultMock.EXPECT().
			Close().
			Return(nil)
		cm.pgxTxMock.EXPECT().
			SendBatch(mock.Anything, mock.Anything).
			Return(cm.pgxBatchResultMock)
		cm.pgxTxMock.EXPECT().
			Commit(mock.Anything).
			Return(nil)
		cm.pgxTxMock.EXPECT().
			Rollback(mock.Anything).
			Return(nil)
		cm.connMock.EXPECT().
			Release().
			Return()

		result, err := Exec(t.Context(), cm.Client, items, prehasher, queryBuilder)
		require.NoError(t, err)
		require.NotNil(t, result)
		require.Equal(t, 2, result.Total)
		require.Equal(t, int64(2), result.RowsAffected)
		require.Contains(t, result.BatchResults, "shard_1")
		require.Contains(t, result.BatchResults, "shard_2")
		require.NoError(t, result.BatchResults.Err())
		require.NoError(t, result.Err())
	})

	t.Run("mapping error", func(t *testing.T) {
		t.Parallel()
		cm := clientMocks(t)
		items := []testItem{{ID: "1", Name: "Alice"}}

		cm.mappingMock.EXPECT().
			GetShard(BucketID(0)).
			Return(mock.Anything, assert.AnError)

		result, err := Exec(t.Context(), cm.Client, items, prehasher, queryBuilder)
		require.Error(t, err)
		require.ErrorIs(t, err, assert.AnError)
		require.Nil(t, result)
	})

	t.Run("get pool error", func(t *testing.T) {
		t.Parallel()
		cm := clientMocks(t)
		items := []testItem{{ID: "1", Name: "Alice"}}

		cm.mappingMock.EXPECT().
			GetShard(BucketID(0)).
			Return("shard_1", nil)

		cm.wrapPoolMock.EXPECT().
			GetPool("shard_1").
			Return(nil, assert.AnError)

		result, err := Exec(context.Background(), cm.Client, items, prehasher, queryBuilder)
		require.NoError(t, err)
		require.NotNil(t, result)
		require.Contains(t, result.BatchResults, "shard_1")
		require.Error(t, result.BatchResults["shard_1"].Err)
		require.Error(t, result.BatchResults.Err())
		require.ErrorIs(t, result.Err(), assert.AnError)
	})

	t.Run("acquire error", func(t *testing.T) {
		t.Parallel()
		cm := clientMocks(t)
		items := []testItem{{ID: "1", Name: "Alice"}}

		cm.mappingMock.EXPECT().
			GetShard(BucketID(0)).
			Return("shard_1", nil)

		cm.wrapPoolMock.EXPECT().
			GetPool("shard_1").
			Return(cm.poolMock, nil)

		cm.poolMock.EXPECT().
			Acquire(mock.Anything).
			Return(nil, assert.AnError)

		result, err := Exec(t.Context(), cm.Client, items, prehasher, queryBuilder)
		require.NoError(t, err)
		require.NotNil(t, result)
		require.Contains(t, result.BatchResults, "shard_1")
		require.Error(t, result.BatchResults["shard_1"].Err)
		require.Error(t, result.BatchResults.Err())
		require.ErrorIs(t, result.Err(), assert.AnError)
	})

	t.Run("begin tx error", func(t *testing.T) {
		t.Parallel()
		cm := clientMocks(t)
		cm.batchTx = true
		items := []testItem{{ID: "1", Name: "Alice"}}

		cm.mappingMock.EXPECT().
			GetShard(BucketID(0)).
			Return("shard_1", nil)

		cm.wrapPoolMock.EXPECT().
			GetPool("shard_1").
			Return(cm.poolMock, nil)

		cm.poolMock.EXPECT().
			Acquire(mock.Anything).
			Return(cm.connMock, nil)

		cm.connMock.EXPECT().
			Begin(mock.Anything).
			Return(nil, assert.AnError)

		cm.connMock.EXPECT().
			Release().
			Return()

		result, err := Exec(t.Context(), cm.Client, items, prehasher, queryBuilder)
		require.NoError(t, err)
		require.NotNil(t, result)
		require.Contains(t, result.BatchResults, "shard_1")
		require.Error(t, result.BatchResults["shard_1"].Err)
		require.Error(t, result.BatchResults.Err())
		require.ErrorIs(t, result.Err(), assert.AnError)
	})

	t.Run("exec error", func(t *testing.T) {
		t.Parallel()
		cm := clientMocks(t)
		cm.batchTx = true
		items := []testItem{{ID: "1", Name: "Alice"}}

		cm.mappingMock.EXPECT().
			GetShard(BucketID(0)).
			Return("shard_1", nil)

		cm.wrapPoolMock.EXPECT().
			GetPool("shard_1").
			Return(cm.poolMock, nil)

		cm.poolMock.EXPECT().
			Acquire(mock.Anything).
			Return(cm.connMock, nil)

		cm.connMock.EXPECT().
			Begin(mock.Anything).
			Return(cm.pgxTxMock, nil)

		cm.pgxTxMock.EXPECT().
			SendBatch(mock.Anything, mock.Anything).
			Return(cm.pgxBatchResultMock)

		cm.pgxBatchResultMock.EXPECT().
			Exec().
			Return(pgconn.CommandTag{}, assert.AnError)

		cm.pgxBatchResultMock.EXPECT().
			Close().
			Return(nil)

		cm.pgxTxMock.EXPECT().
			Rollback(mock.Anything).
			Return(nil)

		cm.connMock.EXPECT().
			Release().
			Return()

		result, err := Exec(t.Context(), cm.Client, items, prehasher, queryBuilder)
		require.NoError(t, err)
		require.NotNil(t, result)
		require.Contains(t, result.BatchResults, "shard_1")
		require.Error(t, result.BatchResults["shard_1"].Err)
		require.Error(t, result.BatchResults.Err())
		require.ErrorIs(t, result.Err(), assert.AnError)
	})

	t.Run("batch close error", func(t *testing.T) {
		t.Parallel()
		cm := clientMocks(t)
		cm.batchTx = true
		items := []testItem{{ID: "1", Name: "Alice"}}

		cm.mappingMock.EXPECT().
			GetShard(BucketID(0)).
			Return("shard_1", nil)

		cm.wrapPoolMock.EXPECT().
			GetPool("shard_1").
			Return(cm.poolMock, nil)

		cm.poolMock.EXPECT().
			Acquire(mock.Anything).
			Return(cm.connMock, nil)

		cm.connMock.EXPECT().
			Begin(mock.Anything).
			Return(cm.pgxTxMock, nil)

		cm.pgxTxMock.EXPECT().
			SendBatch(mock.Anything, mock.Anything).
			Return(cm.pgxBatchResultMock)

		cm.pgxBatchResultMock.EXPECT().
			Exec().
			Return(pgconn.NewCommandTag("INSERT 1"), nil)
		cm.pgxBatchResultMock.EXPECT().
			Close().
			Return(assert.AnError)

		cm.pgxTxMock.EXPECT().
			Rollback(mock.Anything).
			Return(nil)

		cm.connMock.EXPECT().
			Release().
			Return()

		result, err := Exec(t.Context(), cm.Client, items, prehasher, queryBuilder)
		require.NoError(t, err)
		require.NotNil(t, result)
		require.Contains(t, result.BatchResults, "shard_1")
		require.Error(t, result.BatchResults["shard_1"].Err)
		require.Error(t, result.BatchResults.Err())
		require.ErrorIs(t, result.Err(), assert.AnError)
	})

	t.Run("commit error", func(t *testing.T) {
		t.Parallel()
		cm := clientMocks(t)
		cm.batchTx = true
		items := []testItem{{ID: "1", Name: "Alice"}}

		cm.mappingMock.EXPECT().
			GetShard(BucketID(0)).
			Return("shard_1", nil)

		cm.wrapPoolMock.EXPECT().
			GetPool("shard_1").
			Return(cm.poolMock, nil)

		cm.poolMock.EXPECT().
			Acquire(mock.Anything).
			Return(cm.connMock, nil)

		cm.connMock.EXPECT().
			Begin(mock.Anything).
			Return(cm.pgxTxMock, nil)

		cm.pgxTxMock.EXPECT().
			SendBatch(mock.Anything, mock.Anything).
			Return(cm.pgxBatchResultMock)

		cm.pgxBatchResultMock.EXPECT().
			Exec().
			Return(pgconn.NewCommandTag("INSERT 1"), nil)

		cm.pgxBatchResultMock.EXPECT().
			Close().
			Return(nil)

		cm.pgxTxMock.EXPECT().
			Commit(mock.Anything).
			Return(assert.AnError)

		cm.connMock.EXPECT().
			Release().
			Return()

		cm.pgxTxMock.EXPECT().
			Rollback(mock.Anything).
			Return(nil)

		cm.connMock.EXPECT().
			Release().
			Return()

		result, err := Exec(t.Context(), cm.Client, items, prehasher, queryBuilder)
		require.NoError(t, err)
		require.NotNil(t, result)
		require.Contains(t, result.BatchResults, "shard_1")
		require.Error(t, result.BatchResults["shard_1"].Err)
		require.Error(t, result.Err())
		require.ErrorIs(t, result.Err(), assert.AnError)
	})
}
