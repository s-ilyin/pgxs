package pgxs

import (
	"testing"

	"github.com/jackc/pgx/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
)

func TestQuery(t *testing.T) {
	t.Parallel()

	type testItem struct {
		ID   string
		Name string
	}

	prehasher := func(i testItem) []byte { return []byte(i.ID) }
	queryBuilder := func(_ BucketID, i testItem) (string, []any) {
		return `SELECT id FROM {schema}.users WHERE name = $1`, []any{i.Name}
	}

	t.Run("success single shard with tx", func(t *testing.T) {
		t.Parallel()
		cm := clientMocks(t)
		cm.batchTx = true

		src := []testItem{{ID: "1", Name: "Alice"}}

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
			Query().
			Return(cm.pgxRowsMock, nil)

		cm.pgxRowsMock.EXPECT().
			Next().
			Return(true).Once()
		cm.pgxRowsMock.EXPECT().
			Scan(mock.Anything).
			Return(nil).Once()
		cm.pgxRowsMock.EXPECT().
			Next().
			Return(false).Once()
		cm.pgxRowsMock.EXPECT().
			Close().
			Return().Once()
		cm.pgxRowsMock.EXPECT().
			Err().
			Return(nil).Once()

		cm.pgxBatchResultMock.EXPECT().
			Close().
			Return(nil).Times(2)

		cm.pgxTxMock.EXPECT().
			Commit(mock.Anything).
			Return(nil)
		cm.pgxTxMock.EXPECT().
			Rollback(mock.Anything).
			Return(nil)

		cm.connMock.EXPECT().
			Release().
			Return()

		scanRows := func(rows pgx.Rows) (string, error) {
			var id string
			err := rows.Scan(&id)
			return id, err
		}

		results, err := Query(t.Context(), cm.Client, src, prehasher, queryBuilder, scanRows)
		require.NoError(t, err)
		require.Len(t, results, 1)
	})

	t.Run("success single shard without tx", func(t *testing.T) {
		t.Parallel()
		cm := clientMocks(t)
		cm.batchTx = false

		src := []testItem{{ID: "1", Name: "Alice"}}

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
			SendBatch(mock.Anything, mock.Anything).
			Return(cm.pgxBatchResultMock)

		cm.pgxBatchResultMock.EXPECT().
			Query().
			Return(cm.pgxRowsMock, nil)

		cm.pgxRowsMock.EXPECT().
			Next().
			Return(true).Once()
		cm.pgxRowsMock.EXPECT().
			Scan(mock.Anything).
			Return(nil).Once()
		cm.pgxRowsMock.EXPECT().
			Next().
			Return(false).Once()
		cm.pgxRowsMock.EXPECT().
			Close().
			Return().Once()
		cm.pgxRowsMock.EXPECT().
			Err().
			Return(nil).Once()

		cm.pgxBatchResultMock.EXPECT().
			Close().
			Return(nil).Times(2)

		cm.connMock.EXPECT().
			Release().
			Return()

		scanRows := func(rows pgx.Rows) (string, error) {
			var id string
			err := rows.Scan(&id)
			return id, err
		}

		results, err := Query(t.Context(), cm.Client, src, prehasher, queryBuilder, scanRows)
		require.NoError(t, err)
		require.Len(t, results, 1)
	})

	t.Run("success multiple rows returned", func(t *testing.T) {
		t.Parallel()
		cm := clientMocks(t)
		cm.batchTx = false

		src := []testItem{{ID: "1", Name: "Alice"}}

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
			SendBatch(mock.Anything, mock.Anything).
			Return(cm.pgxBatchResultMock)

		cm.pgxBatchResultMock.EXPECT().
			Query().
			Return(cm.pgxRowsMock, nil)

		cm.pgxRowsMock.EXPECT().
			Next().
			Return(true).
			Times(3)
		cm.pgxRowsMock.EXPECT().
			Scan(mock.Anything).
			Return(nil).
			Times(3)
		cm.pgxRowsMock.EXPECT().
			Next().
			Return(false).
			Once()
		cm.pgxRowsMock.EXPECT().
			Close().
			Return().
			Once()
		cm.pgxRowsMock.EXPECT().
			Err().
			Return(nil).
			Once()

		cm.pgxBatchResultMock.EXPECT().
			Close().
			Return(nil).Times(2)

		cm.connMock.EXPECT().
			Release().
			Return()

		scanRows := func(rows pgx.Rows) (string, error) {
			var id string
			err := rows.Scan(&id)
			return id, err
		}

		results, err := Query(t.Context(), cm.Client, src, prehasher, queryBuilder, scanRows)
		require.NoError(t, err)
		require.Len(t, results, 3)
	})

	t.Run("success multiple shards", func(t *testing.T) {
		t.Parallel()
		cm := clientMocks(t)
		cm.batchTx = false

		src := []testItem{
			{ID: "1", Name: "Alice"},
			{ID: "2", Name: "Bob"},
		}

		cm.mappingMock.EXPECT().
			GetShard(BucketID(0)).
			Return("shard_1", nil).Once()
		cm.mappingMock.EXPECT().
			GetShard(BucketID(3)).
			Return("shard_2", nil).Once()

		cm.wrapPoolMock.EXPECT().
			GetPool("shard_1").
			Return(cm.poolMock, nil)
		cm.wrapPoolMock.EXPECT().
			GetPool("shard_2").
			Return(cm.poolMock, nil)

		cm.poolMock.EXPECT().
			Acquire(mock.Anything).
			Return(cm.connMock, nil).Times(2)

		cm.connMock.EXPECT().
			SendBatch(mock.Anything, mock.Anything).
			Return(cm.pgxBatchResultMock).Times(2)

		cm.pgxBatchResultMock.EXPECT().
			Query().
			Return(cm.pgxRowsMock, nil).Times(2)

		cm.pgxRowsMock.EXPECT().
			Next().
			Return(true).Times(2)
		cm.pgxRowsMock.EXPECT().
			Scan(mock.Anything).
			Return(nil).Times(2)
		cm.pgxRowsMock.EXPECT().
			Next().
			Return(false).Times(2)
		cm.pgxRowsMock.EXPECT().
			Close().
			Return().Times(2)
		cm.pgxRowsMock.EXPECT().
			Err().
			Return(nil).Times(2)

		cm.pgxBatchResultMock.EXPECT().
			Close().
			Return(nil).Times(4)

		cm.connMock.EXPECT().
			Release().
			Return().Times(2)

		scanRows := func(rows pgx.Rows) (string, error) {
			var id string
			err := rows.Scan(&id)
			return id, err
		}

		results, err := Query(t.Context(), cm.Client, src, prehasher, queryBuilder, scanRows)
		require.NoError(t, err)
		require.Len(t, results, 2)
	})

	t.Run("empty result set (no rows) is not error", func(t *testing.T) {
		t.Parallel()
		cm := clientMocks(t)
		cm.batchTx = true
		src := []testItem{{ID: "1", Name: "Alice"}}

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
			Query().
			Return(cm.pgxRowsMock, nil)

		cm.pgxRowsMock.EXPECT().
			Next().
			Return(false).Once()
		cm.pgxRowsMock.EXPECT().
			Close().
			Return().Once()
		cm.pgxRowsMock.EXPECT().
			Err().
			Return(nil).Once()

		cm.pgxBatchResultMock.EXPECT().
			Close().
			Return(nil).Times(2)

		cm.pgxTxMock.EXPECT().
			Commit(mock.Anything).
			Return(nil)
		cm.pgxTxMock.EXPECT().
			Rollback(mock.Anything).
			Return(nil)

		cm.connMock.EXPECT().
			Release().
			Return()

		scanRows := func(rows pgx.Rows) (string, error) {
			var id string
			err := rows.Scan(&id)
			return id, err
		}

		results, err := Query(t.Context(), cm.Client, src, prehasher, queryBuilder, scanRows)
		require.NoError(t, err)
		require.Empty(t, results)
	})

	t.Run("mapping error", func(t *testing.T) {
		t.Parallel()
		cm := clientMocks(t)
		src := []testItem{{ID: "1", Name: "Alice"}}

		cm.mappingMock.EXPECT().
			GetShard(BucketID(0)).
			Return(mock.Anything, assert.AnError)

		scanRows := func(rows pgx.Rows) (string, error) {
			var id string
			err := rows.Scan(&id)
			return id, err
		}

		_, err := Query(t.Context(), cm.Client, src, prehasher, queryBuilder, scanRows)
		require.Error(t, err)
		require.ErrorIs(t, err, assert.AnError)
	})

	t.Run("get pool error", func(t *testing.T) {
		t.Parallel()
		cm := clientMocks(t)
		src := []testItem{{ID: "1", Name: "Alice"}}

		cm.mappingMock.EXPECT().
			GetShard(BucketID(0)).
			Return("shard_1", nil)

		cm.wrapPoolMock.EXPECT().
			GetPool("shard_1").
			Return(nil, assert.AnError)

		scanRows := func(rows pgx.Rows) (string, error) {
			var id string
			err := rows.Scan(&id)
			return id, err
		}

		_, err := Query(t.Context(), cm.Client, src, prehasher, queryBuilder, scanRows)
		require.Error(t, err)
		require.ErrorIs(t, err, assert.AnError)
	})

	t.Run("acquire error", func(t *testing.T) {
		t.Parallel()
		cm := clientMocks(t)
		src := []testItem{{ID: "1", Name: "Alice"}}

		cm.mappingMock.EXPECT().
			GetShard(BucketID(0)).
			Return("shard_1", nil)

		cm.wrapPoolMock.EXPECT().
			GetPool("shard_1").
			Return(cm.poolMock, nil)

		cm.poolMock.EXPECT().
			Acquire(mock.Anything).
			Return(nil, assert.AnError)

		scanRows := func(rows pgx.Rows) (string, error) {
			var id string
			err := rows.Scan(&id)
			return id, err
		}

		_, err := Query(t.Context(), cm.Client, src, prehasher, queryBuilder, scanRows)
		require.Error(t, err)
		require.ErrorIs(t, err, assert.AnError)
	})

	t.Run("begin tx error", func(t *testing.T) {
		t.Parallel()
		cm := clientMocks(t)
		cm.batchTx = true
		src := []testItem{{ID: "1", Name: "Alice"}}

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

		scanRows := func(rows pgx.Rows) (string, error) {
			var id string
			err := rows.Scan(&id)
			return id, err
		}

		_, err := Query(t.Context(), cm.Client, src, prehasher, queryBuilder, scanRows)
		require.Error(t, err)
		require.ErrorIs(t, err, assert.AnError)
	})

	t.Run("query error", func(t *testing.T) {
		t.Parallel()
		cm := clientMocks(t)
		cm.batchTx = true
		src := []testItem{{ID: "1", Name: "Alice"}}

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
			Query().
			Return(nil, assert.AnError)

		cm.pgxBatchResultMock.EXPECT().
			Close().
			Return(nil).Times(2)

		cm.pgxTxMock.EXPECT().
			Rollback(mock.Anything).
			Return(nil)

		cm.connMock.EXPECT().
			Release().
			Return()

		scanRows := func(rows pgx.Rows) (string, error) {
			var id string
			err := rows.Scan(&id)
			return id, err
		}

		_, err := Query(t.Context(), cm.Client, src, prehasher, queryBuilder, scanRows)
		require.Error(t, err)
		require.ErrorIs(t, err, assert.AnError)
	})

	t.Run("scan error", func(t *testing.T) {
		t.Parallel()
		cm := clientMocks(t)
		cm.batchTx = true
		src := []testItem{{ID: "1", Name: "Alice"}}

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
			Query().
			Return(cm.pgxRowsMock, nil)

		cm.pgxRowsMock.EXPECT().
			Next().
			Return(true).Once()
		cm.pgxRowsMock.EXPECT().
			Scan(mock.Anything).
			Return(assert.AnError).Once()
		cm.pgxRowsMock.EXPECT().
			Close().
			Return().Times(2)
		cm.pgxRowsMock.EXPECT().
			Err().
			Return(nil).Once()

		cm.pgxBatchResultMock.EXPECT().
			Close().
			Return(nil).Times(2)

		cm.pgxTxMock.EXPECT().
			Rollback(mock.Anything).
			Return(nil)

		cm.connMock.EXPECT().
			Release().
			Return()

		scanRows := func(rows pgx.Rows) (string, error) {
			var id string
			err := rows.Scan(&id)
			return id, err
		}

		_, err := Query(t.Context(), cm.Client, src, prehasher, queryBuilder, scanRows)
		require.Error(t, err)
		require.ErrorIs(t, err, assert.AnError)
	})

	t.Run("scanRows error", func(t *testing.T) {
		t.Parallel()
		cm := clientMocks(t)
		cm.batchTx = true
		src := []testItem{{ID: "1", Name: "Alice"}}

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
			Query().
			Return(cm.pgxRowsMock, nil)

		cm.pgxRowsMock.EXPECT().
			Next().
			Return(true).Once()
		cm.pgxRowsMock.EXPECT().
			Scan(mock.Anything).
			Return(nil).Once()
		cm.pgxRowsMock.EXPECT().
			Close().
			Return().Times(2)
		cm.pgxRowsMock.EXPECT().
			Err().
			Return(nil).Once()

		cm.pgxBatchResultMock.EXPECT().
			Close().
			Return(nil).Times(2)

		cm.pgxTxMock.EXPECT().
			Rollback(mock.Anything).
			Return(nil)

		cm.connMock.EXPECT().
			Release().
			Return()

		badScanRows := func(rows pgx.Rows) (string, error) {
			var id string
			_ = rows.Scan(&id)
			return "", assert.AnError
		}

		_, err := Query(t.Context(), cm.Client, src, prehasher, queryBuilder, badScanRows)
		require.Error(t, err)
		require.ErrorIs(t, err, assert.AnError)
	})

	t.Run("rows iteration error", func(t *testing.T) {
		t.Parallel()
		cm := clientMocks(t)
		cm.batchTx = true
		src := []testItem{{ID: "1", Name: "Alice"}}

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
			Query().
			Return(cm.pgxRowsMock, nil)

		cm.pgxRowsMock.EXPECT().
			Next().
			Return(false).Once()
		cm.pgxRowsMock.EXPECT().
			Close().
			Return().Once()
		cm.pgxRowsMock.EXPECT().
			Err().
			Return(assert.AnError).Once()

		cm.pgxBatchResultMock.EXPECT().
			Close().
			Return(nil).Times(2)

		cm.pgxTxMock.EXPECT().
			Rollback(mock.Anything).
			Return(nil)

		cm.connMock.EXPECT().
			Release().
			Return()

		scanRows := func(rows pgx.Rows) (string, error) {
			var id string
			err := rows.Scan(&id)
			return id, err
		}

		_, err := Query(t.Context(), cm.Client, src, prehasher, queryBuilder, scanRows)
		require.Error(t, err)
		require.ErrorIs(t, err, assert.AnError)
	})

	t.Run("batch result close error", func(t *testing.T) {
		t.Parallel()
		cm := clientMocks(t)
		cm.batchTx = true
		src := []testItem{{ID: "1", Name: "Alice"}}

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
			Query().
			Return(cm.pgxRowsMock, nil)

		cm.pgxRowsMock.EXPECT().
			Next().
			Return(false).Once()
		cm.pgxRowsMock.EXPECT().
			Close().
			Return().Once()
		cm.pgxRowsMock.EXPECT().
			Err().
			Return(nil).Once()

		// Первый Close (явный) вернёт ошибку, второй (defer) — тоже,
		// но loopErr уже установлен, и второй Close не перезапишет его.
		cm.pgxBatchResultMock.EXPECT().
			Close().
			Return(assert.AnError).Times(2)

		cm.pgxTxMock.EXPECT().
			Rollback(mock.Anything).
			Return(nil)

		cm.connMock.EXPECT().
			Release().
			Return()

		scanRows := func(rows pgx.Rows) (string, error) {
			var id string
			err := rows.Scan(&id)
			return id, err
		}

		_, err := Query(t.Context(), cm.Client, src, prehasher, queryBuilder, scanRows)
		require.Error(t, err)
		require.ErrorIs(t, err, assert.AnError)
	})

	t.Run("commit error", func(t *testing.T) {
		t.Parallel()
		cm := clientMocks(t)
		cm.batchTx = true
		src := []testItem{{ID: "1", Name: "Alice"}}

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
			Query().
			Return(cm.pgxRowsMock, nil)

		cm.pgxRowsMock.EXPECT().
			Next().
			Return(false).Once()
		cm.pgxRowsMock.EXPECT().
			Close().
			Return().Once()
		cm.pgxRowsMock.EXPECT().
			Err().
			Return(nil).Once()

		cm.pgxBatchResultMock.EXPECT().
			Close().
			Return(nil).Times(2)

		cm.pgxTxMock.EXPECT().
			Commit(mock.Anything).
			Return(assert.AnError)
		cm.pgxTxMock.EXPECT().
			Rollback(mock.Anything).
			Return(nil)

		cm.connMock.EXPECT().
			Release().
			Return()

		scanRows := func(rows pgx.Rows) (string, error) {
			var id string
			err := rows.Scan(&id)
			return id, err
		}

		_, err := Query(t.Context(), cm.Client, src, prehasher, queryBuilder, scanRows)
		require.Error(t, err)
		require.ErrorIs(t, err, assert.AnError)
	})

	t.Run("src nil", func(t *testing.T) {
		t.Parallel()
		cm := clientMocks(t)
		cm.batchTx = true

		scanRows := func(rows pgx.Rows) (string, error) {
			var id string
			err := rows.Scan(&id)
			return id, err
		}

		results, err := Query(t.Context(), cm.Client, nil, prehasher, queryBuilder, scanRows)
		require.NoError(t, err)
		require.Empty(t, results)
	})

	t.Run("prehasher nil", func(t *testing.T) {
		t.Parallel()
		cm := clientMocks(t)
		cm.batchTx = true
		src := []testItem{{ID: "1", Name: "Alice"}}

		scanRows := func(rows pgx.Rows) (string, error) {
			var id string
			err := rows.Scan(&id)
			return id, err
		}

		_, err := Query(t.Context(), cm.Client, src, nil, queryBuilder, scanRows)
		require.Error(t, err)
	})

	t.Run("query nil", func(t *testing.T) {
		t.Parallel()
		cm := clientMocks(t)
		cm.batchTx = true
		src := []testItem{{ID: "1", Name: "Alice"}}

		scanRows := func(rows pgx.Rows) (string, error) {
			var id string
			err := rows.Scan(&id)
			return id, err
		}

		_, err := Query(t.Context(), cm.Client, src, prehasher, nil, scanRows)
		require.Error(t, err)
	})
}
