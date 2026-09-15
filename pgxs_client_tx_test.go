package pgxs

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
)

func TestClient_BeginBucket(t *testing.T) {
	t.Parallel()

	t.Run("success", func(t *testing.T) {
		t.Parallel()
		cm := clientMocks(t)

		bucketID := BucketID(0)
		cm.mappingMock.EXPECT().
			GetShard(bucketID).
			Return(mock.Anything, nil)
		cm.wrapPoolMock.EXPECT().
			GetPool(mock.Anything).
			Return(cm.poolMock, nil)
		cm.poolMock.EXPECT().
			Begin(mock.Anything).
			Return(cm.pgxTxMock, nil)

		tx, err := cm.BeginBucket(t.Context(), 0)
		require.NoError(t, err)
		require.NotNil(t, tx)

		// Проверяем, что tx является wrapTx с правильной схемой
		impl, ok := tx.(*wrapTx)
		require.True(t, ok)
		require.Equal(t, cm.config.SchemaPrefix+bucketID.String(), impl.schema)
	})

	t.Run("pool not found", func(t *testing.T) {
		t.Parallel()
		cm := clientMocks(t)

		cm.mappingMock.EXPECT().
			GetShard(BucketID(0)).
			Return(mock.Anything, nil)
		cm.wrapPoolMock.EXPECT().
			GetPool(mock.Anything).
			Return(nil, assert.AnError)

		_, err := cm.BeginBucket(t.Context(), 0)
		require.Error(t, err)
		require.ErrorIs(t, err, assert.AnError)
	})

	t.Run("shard not found", func(t *testing.T) {
		t.Parallel()
		cm := clientMocks(t)

		cm.mappingMock.EXPECT().
			GetShard(BucketID(0)).
			Return("", assert.AnError)

		_, err := cm.BeginBucket(t.Context(), 0)
		require.Error(t, err)
		require.ErrorIs(t, err, assert.AnError)
	})

	t.Run("begin error", func(t *testing.T) {
		t.Parallel()
		cm := clientMocks(t)

		cm.mappingMock.EXPECT().
			GetShard(BucketID(0)).
			Return(mock.Anything, nil)
		cm.wrapPoolMock.EXPECT().
			GetPool(mock.Anything).
			Return(cm.poolMock, nil)
		cm.poolMock.EXPECT().
			Begin(mock.Anything).
			Return(nil, assert.AnError)

		_, err := cm.BeginBucket(t.Context(), 0)
		require.Error(t, err)
		require.ErrorIs(t, err, assert.AnError)
	})
}

// --- Begin (с PreHasher) ---

func TestClient_Begin(t *testing.T) {
	t.Parallel()

	t.Run("success", func(t *testing.T) {
		t.Parallel()
		cm := clientMocks(t)
		key := testPrehasher("user_key")

		// Вычисляем bucketID из ключа
		bucketID := BucketID(HashKey(key.PreHash(), cm.config.Buckets))

		cm.mappingMock.EXPECT().
			GetShard(bucketID).
			Return(mock.Anything, nil)
		cm.wrapPoolMock.EXPECT().
			GetPool(mock.Anything).
			Return(cm.poolMock, nil)
		cm.poolMock.EXPECT().
			Begin(mock.Anything).
			Return(cm.pgxTxMock, nil)

		tx, err := cm.BeginPreHasher(t.Context(), key)
		require.NoError(t, err)
		require.NotNil(t, tx)

		impl, ok := tx.(*wrapTx)
		require.True(t, ok)
		require.Equal(t, cm.config.SchemaPrefix+bucketID.String(), impl.schema)
	})

	t.Run("pool not found", func(t *testing.T) {
		t.Parallel()
		cm := clientMocks(t)
		key := testPrehasher("user_key")

		bucketID := BucketID(HashKey(key.PreHash(), 4))
		cm.mappingMock.EXPECT().
			GetShard(bucketID).
			Return(mock.Anything, nil)
		cm.wrapPoolMock.EXPECT().
			GetPool(mock.Anything).
			Return(nil, assert.AnError)

		_, err := cm.BeginPreHasher(t.Context(), key)
		require.Error(t, err)
		require.ErrorIs(t, err, assert.AnError)
	})

	t.Run("shard not found", func(t *testing.T) {
		t.Parallel()
		cm := clientMocks(t)
		key := testPrehasher("user_key")

		bucketID := BucketID(HashKey(key.PreHash(), 4))
		cm.mappingMock.EXPECT().
			GetShard(bucketID).
			Return(mock.Anything, assert.AnError)

		_, err := cm.BeginPreHasher(t.Context(), key)
		require.Error(t, err)
		require.ErrorIs(t, err, assert.AnError)
	})

	t.Run("begin error", func(t *testing.T) {
		t.Parallel()
		cm := clientMocks(t)
		key := testPrehasher("user_key")

		bucketID := BucketID(HashKey(key.PreHash(), 4))
		cm.mappingMock.EXPECT().
			GetShard(bucketID).
			Return(mock.Anything, nil)
		cm.wrapPoolMock.EXPECT().
			GetPool(mock.Anything).
			Return(cm.poolMock, nil)
		cm.poolMock.EXPECT().
			Begin(mock.Anything).
			Return(nil, assert.AnError)

		_, err := cm.BeginPreHasher(t.Context(), key)
		require.Error(t, err)
		require.ErrorIs(t, err, assert.AnError)
	})
}
