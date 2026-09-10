package pgxs

import (
	"context"
	"fmt"
	"time"
)

func withRetryErr(
	ctx context.Context,
	cfg RetryConfig,
	fn func() error,
) error {
	var lastErr error

	for attempt := range cfg.MaxAttempts {
		// Проверяем контекст перед вызовом fn
		if err := ctx.Err(); err != nil {
			return err
		}

		if attempt > 0 {
			if !wait(ctx, cfg, attempt) {
				return ctx.Err()
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

	return fmt.Errorf("with retry: %w", &RetryError{
		Err:      lastErr,
		Attempts: cfg.MaxAttempts,
	})
}

// withRetry — универсальная функция для выполнения операций с повторами.
// fn — функция, которая выполняет операцию и возвращает результат и ошибку.
func withRetry[T any](
	ctx context.Context,
	cfg RetryConfig,
	fn func() (T, error),
) (T, error) {
	var zero T
	var lastErr error

	for attempt := range cfg.MaxAttempts {
		// Проверяем контекст перед вызовом fn
		if err := ctx.Err(); err != nil {
			return zero, err
		}

		if attempt > 0 {
			if !wait(ctx, cfg, attempt) {
				return zero, ctx.Err()
			}
		}

		result, err := fn()
		if err == nil {
			return result, nil
		}

		lastErr = err
		if !isRetryable(err) {
			return zero, err
		}
	}

	return zero, fmt.Errorf("with retry: %w", &RetryError{
		Err:      lastErr,
		Attempts: cfg.MaxAttempts,
	})
}

// wait — экспоненциальная задержка между попытками.
func wait(ctx context.Context, cfg RetryConfig, attempt int) bool {
	delay := min(cfg.BaseDelay*time.Duration(1<<attempt), cfg.MaxDelay)
	select {
	case <-ctx.Done():
		return false
	case <-time.After(delay):
		return true
	}
}
