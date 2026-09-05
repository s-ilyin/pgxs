package pgxs

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

// PoolManager управляет пулами соединений для всех шардов.
type wrapPool struct {
	mu     sync.RWMutex
	pools  map[string]*pgxpool.Pool // имя шарда -> пул
	config *Config
	opts   []PoolOption // опции, применённые при создании
}

// PoolOption определяет функцию настройки конфигурации пула.
type PoolOption func(*pgxpool.Config)

// WithPoolMaxConns устанавливает максимальное количество соединений в пуле.
func WithPoolMaxConns(max int) PoolOption {
	return func(cfg *pgxpool.Config) {
		if max > 0 {
			cfg.MaxConns = int32(max)
		}
	}
}

// WithPoolMinConns устанавливает минимальное количество соединений в пуле.
func WithPoolMinConns(min int) PoolOption {
	return func(cfg *pgxpool.Config) {
		if min >= 0 {
			cfg.MinConns = int32(min)
		}
	}
}

// WithPoolMaxConnIdleTime устанавливает время, после которого неактивное соединение закрывается.
func WithPoolMaxConnIdleTime(d time.Duration) PoolOption {
	return func(cfg *pgxpool.Config) {
		if d > 0 {
			cfg.MaxConnIdleTime = d
		}
	}
}

// WithPoolHealthCheckPeriod устанавливает интервал проверки здоровья соединений.
func WithPoolHealthCheckPeriod(d time.Duration) PoolOption {
	return func(cfg *pgxpool.Config) {
		if d > 0 {
			cfg.HealthCheckPeriod = d
		}
	}
}

// WithPoolMaxConnLifetime устанавливает максимальное время жизни соединения.
func WithPoolMaxConnLifetime(d time.Duration) PoolOption {
	return func(cfg *pgxpool.Config) {
		if d > 0 {
			cfg.MaxConnLifetime = d
		}
	}
}

// WithPoolConnConfig позволяет передать дополнительные настройки pgx.ConnConfig.
// Это расширенная опция для тонкой настройки (например, TLS, пользовательские параметры).
func WithPoolConnConfig(fn func(*pgxpool.Config)) PoolOption {
	return fn
}

// NewPoolManager создаёт менеджер пулов с заданными опциями.
// Если какой-либо шард недоступен, возвращается ошибка, а все уже созданные пулы закрываются.
func newPool(ctx context.Context, cfg *Config, opts ...PoolOption) (*wrapPool, error) {
	pm := &wrapPool{
		pools:  make(map[string]*pgxpool.Pool, len(cfg.Shards)),
		config: cfg,
		opts:   opts,
	}

	for _, shard := range cfg.Shards {
		// Сначала парсим DSN в базовую конфигурацию
		poolConfig, err := pgxpool.ParseConfig(shard.DSN)
		if err != nil {
			pm.Close()
			return nil, fmt.Errorf("failed to parse DSN for shard %s: %w", shard.Name, err)
		}

		// Применяем все опции
		for _, opt := range opts {
			opt(poolConfig)
		}

		// Создаём пул с готовой конфигурацией
		pool, err := pgxpool.NewWithConfig(ctx, poolConfig)
		if err != nil {
			pm.Close()
			return nil, fmt.Errorf("failed to create pool for shard %s: %w", shard.Name, err)
		}

		// Проверяем доступность
		if err := pool.Ping(ctx); err != nil {
			pool.Close()
			pm.Close()
			return nil, fmt.Errorf("shard %s is not reachable: %w", shard.Name, err)
		}

		pm.pools[shard.Name] = pool
	}

	return pm, nil
}

// GetPool возвращает pgxpool.Pool для указанного шарда.
// Если шард не найден, возвращает ошибку.
func (wp *wrapPool) GetPool(shardName string) (*pgxpool.Pool, error) {
	wp.mu.RLock()
	defer wp.mu.RUnlock()

	pool, ok := wp.pools[shardName]
	if !ok {
		return nil, fmt.Errorf("shard %q not found", shardName)
	}
	return pool, nil
}

// Stats возвращает статистику по всем пулам (имя шарда -> pgxpool.Stat).
func (wp *wrapPool) Stats() map[string]*pgxpool.Stat {
	wp.mu.RLock()
	defer wp.mu.RUnlock()

	stats := make(map[string]*pgxpool.Stat, len(wp.pools))
	for name, pool := range wp.pools {
		stats[name] = pool.Stat()
	}
	return stats
}

// Close закрывает все пулы и очищает карту.
func (wp *wrapPool) Close() {
	wp.mu.Lock()
	defer wp.mu.Unlock()

	for _, pool := range wp.pools {
		pool.Close()
	}
	wp.pools = nil
}
