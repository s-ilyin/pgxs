package pgxs

import (
	"context"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
)

// retryBatchResults — обёртка над pgx.BatchResults с повторами.
type retryBatchResults struct {
	ctx      context.Context
	br       pgx.BatchResults
	retryCfg RetryConfig
}

// Exec выполняет следующий запрос в батче с повторами.
func (rbr *retryBatchResults) Exec() (pgconn.CommandTag, error) {
	return withRetry(rbr.ctx, rbr.retryCfg, func() (pgconn.CommandTag, error) {
		return rbr.br.Exec()
	})
}

// Query выполняет следующий запрос в батче с повторами (возвращает строки).
func (rbr *retryBatchResults) Query() (pgx.Rows, error) {
	return withRetry(rbr.ctx, rbr.retryCfg, func() (pgx.Rows, error) {
		return rbr.br.Query()
	})
}

// QueryRow выполняет следующий запрос в батче с повторами (одна строка).
func (rbr *retryBatchResults) QueryRow() pgx.Row {
	return &retryRow{
		ctx:      rbr.ctx,
		row:      rbr.br.QueryRow(),
		retryCfg: rbr.retryCfg,
	}
}

// Close закрывает результаты батча.
func (rbr *retryBatchResults) Close() error {
	return rbr.br.Close()
}
