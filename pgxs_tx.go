package pgxs

import (
	"context"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
)

// Tx представляет транзакцию, привязанную к одному бакету.
type Tx interface {
	Exec(ctx context.Context, sql string, args ...any) (pgconn.CommandTag, error)
	Query(ctx context.Context, sql string, args ...any) (pgx.Rows, error)
	QueryRow(ctx context.Context, sql string, args ...any) pgx.Row
	Commit(ctx context.Context) error
	Rollback(ctx context.Context) error
}

// wrapTx внутренняя реализация Tx.
type wrapTx struct {
	tx     pgx.Tx
	schema string
	client *Client
}

func (t *wrapTx) Exec(ctx context.Context, sql string, args ...any) (pgconn.CommandTag, error) {
	sql = t.client.replaceSchema(sql, t.schema)
	return t.tx.Exec(ctx, sql, args...)
}

func (t *wrapTx) Query(ctx context.Context, sql string, args ...any) (pgx.Rows, error) {
	sql = t.client.replaceSchema(sql, t.schema)
	return t.tx.Query(ctx, sql, args...)
}

func (t *wrapTx) QueryRow(ctx context.Context, sql string, args ...any) pgx.Row {
	sql = t.client.replaceSchema(sql, t.schema)
	return t.tx.QueryRow(ctx, sql, args...)
}

func (t *wrapTx) Commit(ctx context.Context) error {
	return t.tx.Commit(ctx)
}

func (t *wrapTx) Rollback(ctx context.Context) error {
	return t.tx.Rollback(ctx)
}
