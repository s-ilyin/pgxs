package pgxs

import (
	"context"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
)

// RetryPool — обёртка над pgxpool.Pool с автоматическими повторами.
type retryPool struct {
	pool     *pgxpool.Pool
	retryCfg RetryConfig
}

// NewRetryPool создаёт обёртку с заданной конфигурацией повторов.
func NewRetryPool(pool *pgxpool.Pool, cfg RetryConfig) RetryPool {
	return &retryPool{
		pool:     pool,
		retryCfg: cfg,
	}
}

func (rp *retryPool) Stat() *pgxpool.Stat {
	return rp.pool.Stat()
}

// SendBatch отправляет батч с повторами.
func (rc *retryPool) SendBatch(ctx context.Context, batch *pgx.Batch) pgx.BatchResults {
	return rc.pool.SendBatch(ctx, batch)
}

// Exec выполняет запрос с повторами.
func (rp *retryPool) Exec(ctx context.Context, sql string, args ...any) (pgconn.CommandTag, error) {
	return withRetry(ctx, rp.retryCfg, func() (pgconn.CommandTag, error) {
		return rp.pool.Exec(ctx, sql, args...)
	})
}

// Query выполняет запрос с повторами.
func (rp *retryPool) Query(ctx context.Context, sql string, args ...any) (pgx.Rows, error) {
	return withRetry(ctx, rp.retryCfg, func() (pgx.Rows, error) {
		return rp.pool.Query(ctx, sql, args...)
	})
}

// QueryRow возвращает обёртку с ретраями для сканирования одной строки.
func (rp *retryPool) QueryRow(ctx context.Context, sql string, args ...any) pgx.Row {
	return rp.pool.QueryRow(ctx, sql, args...)
}

// Ping проверяет доступность пула с повторами.
func (rp *retryPool) Ping(ctx context.Context) error {
	return rp.pool.Ping(ctx)
}

// Acquire возвращает соединение с повторами.
func (rp *retryPool) Acquire(ctx context.Context) (RetryConn, error) {
	return rp.pool.Acquire(ctx)
}

// Begin начинает транзакцию с повторами.
func (rp *retryPool) Begin(ctx context.Context) (pgx.Tx, error) {
	return rp.pool.Begin(ctx)
}

// BeginTx начинает транзакцию с опциями и повторами.
func (rp *retryPool) BeginTx(ctx context.Context, txOptions pgx.TxOptions) (pgx.Tx, error) {
	return rp.pool.BeginTx(ctx, txOptions)
}

// Close закрывает пул.
func (rp *retryPool) Close() {
	rp.pool.Close()
}
