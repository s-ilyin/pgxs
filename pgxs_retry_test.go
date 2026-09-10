package pgxs

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgconn"
	"github.com/stretchr/testify/require"
)

// --- Тесты withRetryErr ---

func TestWithRetryErr(t *testing.T) {
	t.Parallel()

	cfg := RetryConfig{
		MaxAttempts: 3,
		BaseDelay:   10 * time.Millisecond,
		MaxDelay:    50 * time.Millisecond,
	}

	t.Run("success on first attempt", func(t *testing.T) {
		t.Parallel()
		ctx := context.Background()
		calls := 0
		err := withRetryErr(ctx, cfg, func() error {
			calls++
			return nil
		})
		require.NoError(t, err)
		require.Equal(t, 1, calls)
	})

	t.Run("retryable error then success", func(t *testing.T) {
		t.Parallel()
		ctx := context.Background()
		calls := 0
		deadlock := &pgconn.PgError{Code: "40001"}
		err := withRetryErr(ctx, cfg, func() error {
			calls++
			if calls == 1 {
				return deadlock
			}
			return nil
		})
		require.NoError(t, err)
		require.Equal(t, 2, calls)
	})

	t.Run("exhaust retries with retryable errors", func(t *testing.T) {
		t.Parallel()
		ctx := context.Background()
		calls := 0
		deadlock := &pgconn.PgError{Code: "40P01"}
		err := withRetryErr(ctx, cfg, func() error {
			calls++
			return deadlock
		})
		require.Error(t, err)
		require.Contains(t, err.Error(), "with retry")
		require.Contains(t, err.Error(), "retry failed after 3 attempts")
		require.Equal(t, cfg.MaxAttempts, calls)
	})

	t.Run("non-retryable error", func(t *testing.T) {
		t.Parallel()
		ctx := context.Background()
		calls := 0
		fatal := errors.New("fatal")
		err := withRetryErr(ctx, cfg, func() error {
			calls++
			return fatal
		})
		require.Error(t, err)
		require.Equal(t, fatal, err)
		require.Equal(t, 1, calls)
	})

	t.Run("context deadline exceeded is not retryable", func(t *testing.T) {
		t.Parallel()
		ctx := context.Background()
		calls := 0
		err := withRetryErr(ctx, cfg, func() error {
			calls++
			return context.DeadlineExceeded
		})
		require.ErrorIs(t, err, context.DeadlineExceeded)
		require.Equal(t, 1, calls) // без повторов
	})

	t.Run("context canceled is not retryable", func(t *testing.T) {
		t.Parallel()
		ctx := context.Background()
		calls := 0
		err := withRetryErr(ctx, cfg, func() error {
			calls++
			return context.Canceled
		})
		require.ErrorIs(t, err, context.Canceled)
		require.Equal(t, 1, calls)
	})

	t.Run("context canceled before any attempt", func(t *testing.T) {
		t.Parallel()
		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		calls := 0
		err := withRetryErr(ctx, cfg, func() error {
			calls++
			return nil
		})
		require.ErrorIs(t, err, context.Canceled)
		require.Equal(t, 0, calls) // fn не вызывается
	})

	t.Run("context canceled during wait", func(t *testing.T) {
		t.Parallel()
		ctx, cancel := context.WithCancel(context.Background())
		calls := 0
		deadlock := &pgconn.PgError{Code: "40001"}
		go func() {
			time.Sleep(5 * time.Millisecond)
			cancel()
		}()
		err := withRetryErr(ctx, cfg, func() error {
			calls++
			return deadlock // retryable, чтобы уйти в wait
		})
		require.ErrorIs(t, err, context.Canceled)
		require.Equal(t, 1, calls)
	})

	t.Run("retry on serialization failure (40001)", func(t *testing.T) {
		t.Parallel()
		ctx := context.Background()
		calls := 0
		serializationErr := &pgconn.PgError{Code: "40001"}
		err := withRetryErr(ctx, cfg, func() error {
			calls++
			if calls == 1 {
				return serializationErr
			}
			return nil
		})
		require.NoError(t, err)
		require.Equal(t, 2, calls)
	})

	t.Run("retry on connection error (08xxx)", func(t *testing.T) {
		t.Parallel()
		ctx := context.Background()
		calls := 0
		connErr := &pgconn.PgError{Code: "08006"}
		err := withRetryErr(ctx, cfg, func() error {
			calls++
			if calls == 2 {
				return nil
			}
			return connErr
		})
		require.NoError(t, err)
		require.Equal(t, 2, calls)
	})
}

// --- Тесты withRetry (обобщённая) ---

func TestWithRetry(t *testing.T) {
	t.Parallel()

	cfg := RetryConfig{
		MaxAttempts: 3,
		BaseDelay:   10 * time.Millisecond,
		MaxDelay:    50 * time.Millisecond,
	}

	t.Run("success on first attempt", func(t *testing.T) {
		t.Parallel()
		ctx := context.Background()
		calls := 0
		res, err := withRetry(ctx, cfg, func() (int, error) {
			calls++
			return 42, nil
		})
		require.NoError(t, err)
		require.Equal(t, 42, res)
		require.Equal(t, 1, calls)
	})

	t.Run("retryable error then success", func(t *testing.T) {
		t.Parallel()
		ctx := context.Background()
		calls := 0
		deadlock := &pgconn.PgError{Code: "40P01"}
		res, err := withRetry(ctx, cfg, func() (int, error) {
			calls++
			if calls == 1 {
				return 0, deadlock
			}
			return 42, nil
		})
		require.NoError(t, err)
		require.Equal(t, 42, res)
		require.Equal(t, 2, calls)
	})

	t.Run("exhaust retries returns zero value", func(t *testing.T) {
		t.Parallel()
		ctx := context.Background()
		calls := 0
		deadlock := &pgconn.PgError{Code: "40001"}
		res, err := withRetry(ctx, cfg, func() (int, error) {
			calls++
			return 0, deadlock
		})
		require.Error(t, err)
		require.Equal(t, 0, res) // zero value
		require.Equal(t, cfg.MaxAttempts, calls)
	})

	t.Run("non-retryable error returns immediately", func(t *testing.T) {
		t.Parallel()
		ctx := context.Background()
		calls := 0
		fatal := errors.New("fatal")
		_, err := withRetry(ctx, cfg, func() (int, error) {
			calls++
			return 0, fatal
		})
		require.ErrorIs(t, err, fatal)
		require.Equal(t, 1, calls)
	})

	t.Run("context deadline exceeded is not retryable", func(t *testing.T) {
		t.Parallel()
		ctx := context.Background()
		calls := 0
		_, err := withRetry(ctx, cfg, func() (int, error) {
			calls++
			return 0, context.DeadlineExceeded
		})
		require.ErrorIs(t, err, context.DeadlineExceeded)
		require.Equal(t, 1, calls)
	})

	t.Run("context canceled before any attempt", func(t *testing.T) {
		t.Parallel()
		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		calls := 0
		_, err := withRetry(ctx, cfg, func() (int, error) {
			calls++
			return 0, nil
		})
		require.ErrorIs(t, err, context.Canceled)
		require.Equal(t, 0, calls)
	})

	t.Run("context canceled during wait", func(t *testing.T) {
		t.Parallel()
		ctx, cancel := context.WithCancel(context.Background())
		calls := 0
		deadlock := &pgconn.PgError{Code: "40001"}
		go func() {
			time.Sleep(5 * time.Millisecond)
			cancel()
		}()
		_, err := withRetry(ctx, cfg, func() (int, error) {
			calls++
			return 0, deadlock
		})
		require.ErrorIs(t, err, context.Canceled)
		require.Equal(t, 1, calls)
	})
}

// --- Тесты wait ---

func TestWait(t *testing.T) {
	t.Parallel()

	cfg := RetryConfig{
		MaxAttempts: 3,
		BaseDelay:   10 * time.Millisecond,
		MaxDelay:    50 * time.Millisecond,
	}

	t.Run("returns true after delay", func(t *testing.T) {
		t.Parallel()
		ctx := context.Background()
		start := time.Now()
		ok := wait(ctx, cfg, 1)
		elapsed := time.Since(start)
		require.True(t, ok)
		// delay = BaseDelay * 2^1 = 20ms, должно быть >= 15ms (учёт планировщика)
		require.GreaterOrEqual(t, elapsed, 15*time.Millisecond)
	})

	t.Run("returns false if context canceled", func(t *testing.T) {
		t.Parallel()
		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		ok := wait(ctx, cfg, 1)
		require.False(t, ok)
	})

	t.Run("caps delay at MaxDelay", func(t *testing.T) {
		t.Parallel()
		ctx := context.Background()
		// attempt = 10 => BaseDelay * 2^10 = 10ms * 1024 = ~10s, но MaxDelay = 50ms
		start := time.Now()
		ok := wait(ctx, cfg, 10)
		elapsed := time.Since(start)
		require.True(t, ok)
		// Должно быть около MaxDelay (50ms), но не больше
		require.Less(t, elapsed, 200*time.Millisecond)
	})
}

// --- Тесты isRetryable ---

func TestIsRetryable(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		err  error
		want bool
	}{
		{"nil", nil, false},
		{"context deadline is not retryable", context.DeadlineExceeded, false},
		{"context canceled is not retryable", context.Canceled, false},
		{"connection error 08xxx", &pgconn.PgError{Code: "08000"}, true},
		{"deadlock 40P01", &pgconn.PgError{Code: "40P01"}, true},
		{"serialization 40001", &pgconn.PgError{Code: "40001"}, true},
		{"query canceled 57014", &pgconn.PgError{Code: "57014"}, true},
		{"syntax error 42601", &pgconn.PgError{Code: "42601"}, false},
		{"unique violation 23505", &pgconn.PgError{Code: "23505"}, false},
		{"permission denied 42501", &pgconn.PgError{Code: "42501"}, false},
		{"unknown error", errors.New("some error"), false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := isRetryable(tt.err)
			require.Equal(t, tt.want, got)
		})
	}
}

// --- Тесты RetryError ---

func TestRetryError(t *testing.T) {
	t.Parallel()

	t.Run("Error and Unwrap", func(t *testing.T) {
		inner := errors.New("inner error")
		re := &RetryError{Err: inner, Attempts: 3}

		require.Contains(t, re.Error(), "retry failed after 3 attempts")
		require.Contains(t, re.Error(), "inner error")
		require.ErrorIs(t, re, inner) // Unwrap работает
	})
}
