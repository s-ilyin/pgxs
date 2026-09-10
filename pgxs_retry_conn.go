package pgxs

import (
	"context"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
)

// RetryConn — обёртка над pgxpool.Conn с повторами.
type retryConn struct {
	conn     *pgxpool.Conn
	retryCfg RetryConfig
}

// Exec выполняет запрос с повторами.
func (rc *retryConn) Exec(ctx context.Context, sql string, args ...any) (pgconn.CommandTag, error) {
	return withRetry(ctx, rc.retryCfg, func() (pgconn.CommandTag, error) {
		return rc.conn.Exec(ctx, sql, args...)
	})
}

// Query выполняет запрос с повторами.
func (rc *retryConn) Query(ctx context.Context, sql string, args ...any) (pgx.Rows, error) {
	return withRetry(ctx, rc.retryCfg, func() (pgx.Rows, error) {
		return rc.conn.Query(ctx, sql, args...)
	})
}

// QueryRow возвращает обёртку с ретраями для одной строки.
func (rc *retryConn) QueryRow(ctx context.Context, sql string, args ...any) pgx.Row {
	return rc.conn.QueryRow(ctx, sql, args...)
}

// SendBatch отправляет батч с повторами.
func (rc *retryConn) SendBatch(ctx context.Context, batch *pgx.Batch) pgx.BatchResults {
	return rc.conn.SendBatch(ctx, batch)
}

// BeginTx начинает транзакцию с опциями и повторами.
func (rc *retryConn) BeginTx(ctx context.Context, txOptions pgx.TxOptions) (pgx.Tx, error) {
	return rc.conn.BeginTx(ctx, txOptions)
}

// Begin начинает транзакцию.
func (rc *retryConn) Begin(ctx context.Context) (pgx.Tx, error) {
	return rc.conn.Begin(ctx)
}

// Release освобождает соединение обратно в пул.
func (rc *retryConn) Release() {
	rc.conn.Release()
}
