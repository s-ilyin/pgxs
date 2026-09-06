package pgxs

import (
	"context"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// retryRow — обёртка над pgx.Row с повторами.
type retryRow struct {
	ctx      context.Context
	pool     *pgxpool.Pool
	conn     *pgxpool.Conn
	row      pgx.Row
	sql      string
	args     []any
	retryCfg RetryConfig
}

// withRetryRow — универсальная функция для повторов в QueryRow.
func (r *retryRow) withRetryRow(fn func() error) error {
	var lastErr error

	for attempt := range r.retryCfg.MaxAttempts {
		if attempt > 0 {
			delay := min(r.retryCfg.BaseDelay*time.Duration(1<<attempt), r.retryCfg.MaxDelay)
			select {
			case <-r.ctx.Done():
				return r.ctx.Err()
			case <-time.After(delay):
			}
		}

		err := fn()
		if err == nil {
			return nil
		}

		lastErr = err
		if !isRetryable(err) {
			return err
		}
	}

	return fmt.Errorf("with retry row: %w", &RetryError{
		Err:      lastErr,
		Attempts: r.retryCfg.MaxAttempts,
	})
}

// Scan выполняет сканирование строки с повторами.
func (r *retryRow) Scan(dest ...any) error {
	return r.withRetryRow(func() error {
		var row pgx.Row
		if r.conn != nil {
			row = r.conn.QueryRow(r.ctx, r.sql, r.args...)
		} else {
			row = r.pool.QueryRow(r.ctx, r.sql, r.args...)
		}
		return row.Scan(dest...)
	})
}
