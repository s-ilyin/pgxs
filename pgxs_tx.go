package pgxs

import (
	"context"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
)

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

func (t *wrapTx) Begin(ctx context.Context) (pgx.Tx, error) {
	return t.tx.Begin(ctx)
}

func (t *wrapTx) Commit(ctx context.Context) error {
	return t.tx.Commit(ctx)
}

func (t *wrapTx) Rollback(ctx context.Context) error {
	return t.tx.Rollback(ctx)
}

func (t *wrapTx) Conn() *pgx.Conn {
	return t.tx.Conn()
}

func (t *wrapTx) CopyFrom(ctx context.Context, tableName pgx.Identifier, columnNames []string, rowSrc pgx.CopyFromSource) (int64, error) {
	return t.tx.CopyFrom(ctx, tableName, columnNames, rowSrc)
}

func (t *wrapTx) LargeObjects() pgx.LargeObjects {
	return t.tx.LargeObjects()
}

func (t *wrapTx) Prepare(ctx context.Context, name, sql string) (*pgconn.StatementDescription, error) {
	return t.tx.Prepare(ctx, name, sql)
}

func (t *wrapTx) SendBatch(ctx context.Context, b *pgx.Batch) pgx.BatchResults {
	return t.tx.SendBatch(ctx, b)
}
