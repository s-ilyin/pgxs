package pgxs

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"sync"

	"github.com/jackc/pgx/v5"
	"golang.org/x/sync/errgroup"
)

// ForEachRow выполняет один и тот же SQL-запрос на всех бакетах (схемах) и вызывает
// scanRows для каждой строки результата. Все собранные значения возвращаются в одном слайсе.
//
// Важно: scanRows НЕ должна иметь побочных эффектов — при retry (например, при 40001/40P01)
// запрос будет выполнен заново, и буфер попытки при полностью сбрасывается, поэтому дублирования
// значений не произойдёт. Пользователь получает уже собранные результаты.
//
// Все шарды обрабатываются параллельно с ограничением через client.concurrency.
// Возвращает собранные результаты и объединённую ошибку по всем шардам.
func ForEachRow[R any](
	ctx context.Context,
	client *Client,
	scanRows func(pgx.Rows) (R, error),
	sql string,
	args ...any,
) ([]R, error) {
	if scanRows == nil {
		return nil, errors.New("func scanRows is nil")
	}

	// Группируем бакеты по шардам
	groups := make(map[string][]BucketID) // shard -> []bucketID
	for b := range client.config.Buckets {
		shard, err := client.mapping.GetShard(BucketID(b))
		if err != nil {
			return nil, fmt.Errorf("get shard for bucket %d: %w", b, err)
		}
		groups[shard] = append(groups[shard], BucketID(b))
	}
	if len(groups) == 0 {
		return nil, fmt.Errorf("no shards found")
	}

	// Создаём errgroup с отменяемым контекстом
	eg, ctx := errgroup.WithContext(ctx)

	// Ограничиваем параллелизм числом шардов, но не больше maxParallel
	limit := min(client.concurrency, len(groups))
	eg.SetLimit(limit)

	var (
		mu     sync.Mutex
		errs   []error
		result []R
	)

	for shard, buckets := range groups {
		eg.Go(func() error {
			// Локальный буфер попытки. Изолирован между попытками внутри withRetryErr,
			// поэтому при retry дубликатов не будет.
			var shardResult []R

			shardErr := withRetryErr(ctx, client.config.Retry, func() error {
				// Сбрасываем буфер перед каждой попыткой — ключевой момент для retry
				// Переиспользуем underlying array, но сначала обнуляем ссылки —
				// чтобы GC мог освободить объекты, на которые они указывали.
				clear(shardResult)
				shardResult = shardResult[:0]

				pool, err := client.wrapPool.GetPool(shard)
				if err != nil {
					return fmt.Errorf("pool for shard %s: %w", shard, err)
				}

				conn, err := pool.Acquire(ctx)
				if err != nil {
					return fmt.Errorf("acquire connection for shard %s: %w", shard, err)
				}
				defer conn.Release()

				batch := &pgx.Batch{}
				for _, bucket := range buckets {
					schema := client.config.SchemaPrefix + strconv.Itoa(bucket.Int())
					sqlWithSchema := client.replaceSchema(sql, schema)
					batch.Queue(sqlWithSchema, args...)
				}

				br := conn.SendBatch(ctx, batch)
				defer br.Close()

				// Обрабатываем каждый запрос в батче
				for i := range batch.Len() {
					// Проверяем, не отменён ли контекст
					select {
					case <-ctx.Done():
						return ctx.Err()
					default:
					}

					rows, err := br.Query()
					if err != nil {
						return fmt.Errorf("shard %s, bucket %d: %w", shard, buckets[i], err)
					}

					for rows.Next() {
						res, err := scanRows(rows)
						if err != nil {
							rows.Close()
							return fmt.Errorf("scan rows error on shard %s, bucket %d: %w", shard, buckets[i], err)
						}
						shardResult = append(shardResult, res)
					}
					rows.Close()
					if err := rows.Err(); err != nil {
						return fmt.Errorf("shard %s, bucket %d: %w", shard, buckets[i], err)
					}
				}
				return nil
			})

			if shardErr != nil {
				mu.Lock()
				errs = append(errs, fmt.Errorf("shard %s: %w", shard, shardErr))
				mu.Unlock()
				return nil // не прерываем errgroup — другие шарды продолжают работу
			}

			// добавляем результаты шарда в общий слайс
			mu.Lock()
			result = append(result, shardResult...)
			mu.Unlock()
			return nil
		})
	}

	_ = eg.Wait()

	if len(errs) > 0 {
		return nil, errors.Join(errs...)
	}
	return result, nil
}
