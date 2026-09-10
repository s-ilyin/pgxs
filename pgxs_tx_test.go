package pgxs

import (
	"context"
	"testing"

	"github.com/jackc/pgx/v5/pgconn"
	"github.com/s-ilyin/pgxs/mocks"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
)

func TestWrapTx(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	schema := "bucket_0"
	sqlWithPlaceholder := `SELECT * FROM {schema}.users WHERE id = $1`
	expectedSQL := `SELECT * FROM bucket_0.users WHERE id = $1`

	client := &Client{
		schemaReplacer: func(sql, schema string) string {
			return expectedSQL
		},
	}

	t.Run("Exec", func(t *testing.T) {
		t.Parallel()
		mockTx := mocks.NewPgxTxMock(t)
		mockTx.EXPECT().
			Exec(ctx, expectedSQL, []any{"123"}).
			Return(pgconn.NewCommandTag("UPDATE 1"), nil)

		w := &wrapTx{tx: mockTx, schema: schema, client: client}
		tag, err := w.Exec(ctx, sqlWithPlaceholder, "123")
		require.NoError(t, err)
		require.Equal(t, "UPDATE 1", tag.String())
	})

	t.Run("Query", func(t *testing.T) {
		t.Parallel()
		mockTx := mocks.NewPgxTxMock(t)
		mockRows := mocks.NewPgxRowsMock(t)
		mockTx.EXPECT().
			Query(ctx, expectedSQL, []any{"123"}).
			Return(mockRows, nil)

		w := &wrapTx{tx: mockTx, schema: schema, client: client}
		rows, err := w.Query(ctx, sqlWithPlaceholder, "123")
		require.NoError(t, err)
		require.NotNil(t, rows)
	})

	t.Run("QueryRow", func(t *testing.T) {
		t.Parallel()
		mockTx := mocks.NewPgxTxMock(t)
		mockRow := mocks.NewPgxRowMock(t)
		mockTx.EXPECT().
			QueryRow(ctx, expectedSQL, []any{"123"}).
			Return(mockRow)

		mockRow.EXPECT().
			Scan(mock.Anything).
			Return(nil)

		w := &wrapTx{tx: mockTx, schema: schema, client: client}
		row := w.QueryRow(ctx, sqlWithPlaceholder, "123")
		require.NotNil(t, row)

		var id string
		err := row.Scan(&id)
		require.NoError(t, err)
	})

	t.Run("Commit", func(t *testing.T) {
		t.Parallel()
		mockTx := mocks.NewPgxTxMock(t)
		mockTx.EXPECT().
			Commit(mock.Anything).
			Return(nil)

		w := &wrapTx{tx: mockTx, schema: schema, client: client}
		err := w.Commit(ctx)
		require.NoError(t, err)
	})

	t.Run("Rollback", func(t *testing.T) {
		t.Parallel()
		mockTx := mocks.NewPgxTxMock(t)
		mockTx.EXPECT().
			Rollback(mock.Anything).
			Return(nil)

		w := &wrapTx{tx: mockTx, schema: schema, client: client}
		err := w.Rollback(ctx)
		require.NoError(t, err)
	})
}
