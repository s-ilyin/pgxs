package pgxs

import (
	"time"

	"github.com/jackc/pgx/v5/pgconn"
)

const (
	DefaultMaxAttempts = 3
	DefaultBaseDelay   = 100 * time.Millisecond
	DefaultMaxDelay    = 2 * time.Second
)

// RetryConfig — настройки повторов.
type RetryConfig struct {
	MaxAttempts int           // максимальное число попыток (включая первую)
	BaseDelay   time.Duration // начальная задержка между попытками
	MaxDelay    time.Duration // максимальная задержка
}

// DefaultRetryConfig — стандартная конфигурация повторов.
func DefaultRetryConfig() RetryConfig {
	return RetryConfig{
		MaxAttempts: DefaultMaxAttempts,
		BaseDelay:   DefaultBaseDelay,
		MaxDelay:    DefaultMaxDelay,
	}
}

// isRetryable определяет, можно ли повторить операцию после этой ошибки.
func isRetryable(err error) bool {
	if err == nil {
		return false
	}
	// Проверяем SQLSTATE
	if pgErr, ok := err.(*pgconn.PgError); ok {
		// Serialization failure / deadlock
		if pgErr.Code == "40001" || pgErr.Code == "40P01" {
			return true
		}
		// Connection errors (08xxx)
		if len(pgErr.Code) >= 2 && pgErr.Code[:2] == "08" {
			return true
		}
		// Query canceled / timeout
		if pgErr.Code == "57014" {
			return true
		}
	}

	return false
}
