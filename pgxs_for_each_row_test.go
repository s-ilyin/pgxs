package pgxs

import (
	"context"
	"testing"

	"github.com/jackc/pgx/v5"
	"github.com/s-ilyin/pgxs/mocks"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
)

func TestClient_ForEachRow(t *testing.T) {
	t.Parallel()

	t.Run("success with multiple shards and buckets", func(t *testing.T) {
		t.Parallel()
		cm := clientMocks(t)

		// Ожидаем mapping для 4 бакетов: 0,1 → shard_1; 2,3 → shard_2
		cm.mappingMock.EXPECT().
			GetShard(BucketID(0)).
			Return("shard_1", nil)
		cm.mappingMock.EXPECT().
			GetShard(BucketID(1)).
			Return("shard_1", nil)
		cm.mappingMock.EXPECT().
			GetShard(BucketID(2)).
			Return("shard_2", nil)
		cm.mappingMock.EXPECT().
			GetShard(BucketID(3)).
			Return("shard_2", nil)

		// Для shard_1
		cm.wrapPoolMock.EXPECT().
			GetPool("shard_1").
			Return(cm.poolMock, nil)
		cm.poolMock.EXPECT().
			Acquire(mock.Anything).
			Return(cm.connMock, nil)

		// Для shard_2
		cm.wrapPoolMock.EXPECT().
			GetPool("shard_2").
			Return(cm.poolMock, nil)
		cm.poolMock.EXPECT().
			Acquire(mock.Anything).
			Return(cm.connMock, nil)

		// Ожидаем SendBatch для каждого шарда
		cm.connMock.EXPECT().
			SendBatch(mock.Anything, mock.Anything).
			Return(cm.pgxBatchResultMock).Times(2)

		// Создаём 4 отдельных мока Rows для 4 запросов
		rowsMock1 := mocks.NewPgxRowsMock(t)
		rowsMock1.EXPECT().Next().Return(true).Once()
		rowsMock1.EXPECT().Scan(mock.Anything).Return(nil).Once()
		rowsMock1.EXPECT().Next().Return(false).Once()
		rowsMock1.EXPECT().Close().Return().Once()
		rowsMock1.EXPECT().Err().Return(nil).Once()

		rowsMock2 := mocks.NewPgxRowsMock(t)
		rowsMock2.EXPECT().Next().Return(true).Once()
		rowsMock2.EXPECT().Scan(mock.Anything).Return(nil).Once()
		rowsMock2.EXPECT().Next().Return(false).Once()
		rowsMock2.EXPECT().Close().Return().Once()
		rowsMock2.EXPECT().Err().Return(nil).Once()

		rowsMock3 := mocks.NewPgxRowsMock(t)
		rowsMock3.EXPECT().Next().Return(true).Once()
		rowsMock3.EXPECT().Scan(mock.Anything).Return(nil).Once()
		rowsMock3.EXPECT().Next().Return(false).Once()
		rowsMock3.EXPECT().Close().Return().Once()
		rowsMock3.EXPECT().Err().Return(nil).Once()

		rowsMock4 := mocks.NewPgxRowsMock(t)
		rowsMock4.EXPECT().Next().Return(true).Once()
		rowsMock4.EXPECT().Scan(mock.Anything).Return(nil).Once()
		rowsMock4.EXPECT().Next().Return(false).Once()
		rowsMock4.EXPECT().Close().Return().Once()
		rowsMock4.EXPECT().Err().Return(nil).Once()

		// Ожидаем Query для каждого батча (по 2 запроса на шард)
		cm.pgxBatchResultMock.EXPECT().
			Query().
			Return(rowsMock1, nil).Once()
		cm.pgxBatchResultMock.EXPECT().
			Query().
			Return(rowsMock2, nil).Once()
		cm.pgxBatchResultMock.EXPECT().
			Query().
			Return(rowsMock3, nil).Once()
		cm.pgxBatchResultMock.EXPECT().
			Query().
			Return(rowsMock4, nil).Once()

		// Ожидаем Close для каждого батча
		cm.pgxBatchResultMock.EXPECT().
			Close().
			Return(nil).Times(2)

		// Ожидаем Release для каждого соединения
		cm.connMock.EXPECT().
			Release().
			Return().Times(2)

		var scannedIDs []string
		err := cm.ForEachRow(
			context.Background(),
			func(rows pgx.Rows) error {
				var id string
				if err := rows.Scan(&id); err != nil {
					return err
				}
				scannedIDs = append(scannedIDs, id)
				return nil
			},
			`SELECT id FROM {schema}.users WHERE name = $1`,
			"Alice",
		)
		require.NoError(t, err)
		require.Len(t, scannedIDs, 4)
	})

	t.Run("error mapping fails", func(t *testing.T) {
		t.Parallel()
		cm := clientMocks(t)

		cm.mappingMock.EXPECT().
			GetShard(BucketID(0)).
			Return(mock.Anything, assert.AnError)

		err := cm.ForEachRow(
			context.Background(),
			func(rows pgx.Rows) error { return nil },
			`SELECT id FROM {schema}.users`,
		)
		require.Error(t, err)
		require.ErrorIs(t, err, assert.AnError)
	})

	t.Run("error get pool fails", func(t *testing.T) {
		t.Parallel()
		cm := clientMocks(t)

		// Ожидаем mapping для всех бакетов (происходит до горутин)
		for b := range 4 {
			shard := "shard_1"
			cm.mappingMock.EXPECT().
				GetShard(BucketID(b)).
				Return(shard, nil)
		}

		cm.wrapPoolMock.EXPECT().
			GetPool("shard_1").
			Return(nil, assert.AnError)

		err := cm.ForEachRow(
			context.Background(),
			func(rows pgx.Rows) error { return nil },
			`SELECT id FROM {schema}.users`,
		)
		require.Error(t, err)
		require.ErrorIs(t, err, assert.AnError)
	})

	t.Run("error acquire connection fails", func(t *testing.T) {
		t.Parallel()
		cm := clientMocks(t)

		for b := range 4 {
			shard := "shard_1"
			cm.mappingMock.EXPECT().
				GetShard(BucketID(b)).
				Return(shard, nil)
		}

		cm.wrapPoolMock.EXPECT().
			GetPool("shard_1").
			Return(cm.poolMock, nil)

		cm.poolMock.EXPECT().
			Acquire(mock.Anything).
			Return(nil, assert.AnError)

		err := cm.ForEachRow(
			context.Background(),
			func(rows pgx.Rows) error { return nil },
			`SELECT id FROM {schema}.users`,
		)
		require.Error(t, err)
		require.ErrorIs(t, err, assert.AnError)
	})

	t.Run("error batch result Query fails", func(t *testing.T) {
		t.Parallel()
		cm := clientMocks(t)

		// Все бакеты направляем на shard_1 для простоты
		for b := range 4 {
			cm.mappingMock.EXPECT().
				GetShard(BucketID(b)).
				Return("shard_1", nil)
		}
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
			Return(nil, assert.AnError)

		cm.pgxBatchResultMock.EXPECT().
			Close().
			Return(nil)

		cm.connMock.EXPECT().
			Release().
			Return()

		err := cm.ForEachRow(
			context.Background(),
			func(rows pgx.Rows) error { return nil },
			`SELECT id FROM {schema}.users`,
		)
		require.Error(t, err)
		require.ErrorIs(t, err, assert.AnError)
	})

	t.Run("error scanRows fails", func(t *testing.T) {
		t.Parallel()
		cm := clientMocks(t)

		for b := range 4 {
			cm.mappingMock.EXPECT().
				GetShard(BucketID(b)).
				Return("shard_1", nil)
		}
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
			Scan(mock.Anything).Return(assert.AnError).Once()

		cm.pgxRowsMock.EXPECT().
			Close().Return().Once()

		cm.pgxBatchResultMock.EXPECT().
			Close().
			Return(nil)

		cm.connMock.EXPECT().
			Release().
			Return()

		err := cm.ForEachRow(
			context.Background(),
			func(rows pgx.Rows) error {
				var id string
				return rows.Scan(&id)
			},
			`SELECT id FROM {schema}.users`,
		)
		require.Error(t, err)
		require.ErrorIs(t, err, assert.AnError)
	})

	t.Run("error rows.Err after iteration", func(t *testing.T) {
		t.Parallel()
		cm := clientMocks(t)

		for b := range 4 {
			cm.mappingMock.EXPECT().
				GetShard(BucketID(b)).
				Return("shard_1", nil)
		}
		cm.wrapPoolMock.EXPECT().
			GetPool("shard_1").
			Return(cm.poolMock, nil)
		cm.poolMock.EXPECT().
			Acquire(mock.Anything).
			Return(cm.connMock, nil)

		cm.connMock.EXPECT().
			SendBatch(mock.Anything, mock.Anything).
			Return(cm.pgxBatchResultMock)

		rowsMock := mocks.NewPgxRowsMock(t)
		rowsMock.EXPECT().Next().Return(false).Once()
		rowsMock.EXPECT().Close().Return().Once()
		rowsMock.EXPECT().Err().Return(assert.AnError).Once()

		cm.pgxBatchResultMock.EXPECT().
			Query().
			Return(rowsMock, nil)

		cm.pgxBatchResultMock.EXPECT().
			Close().
			Return(nil)

		cm.connMock.EXPECT().
			Release().
			Return()

		err := cm.ForEachRow(
			context.Background(),
			func(rows pgx.Rows) error { return nil },
			`SELECT id FROM {schema}.users`,
		)
		require.Error(t, err)
		require.ErrorIs(t, err, assert.AnError)
	})

	t.Run("context cancelled", func(t *testing.T) {
		t.Parallel()
		cm := clientMocks(t)
		ctx, cancel := context.WithCancel(context.Background())
		cancel()

		for b := range 4 {
			cm.mappingMock.EXPECT().
				GetShard(BucketID(b)).
				Return("shard_1", nil)
		}

		err := cm.ForEachRow(
			ctx,
			func(rows pgx.Rows) error { return nil },
			`SELECT id FROM {schema}.users`,
		)
		require.Error(t, err)
		require.ErrorIs(t, err, context.Canceled)
	})
}
