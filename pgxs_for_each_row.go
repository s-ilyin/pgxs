package pgxs

import (
	"context"
	"fmt"
	"strconv"

	"github.com/jackc/pgx/v5"
	"golang.org/x/sync/errgroup"
)

// ---- EachRow (параллельные запросы на всех бакетах) ----

// ForEachRow выполняет Query на всех бакетах и вызывает handler для каждой строки.
// Каждый шард обрабатывается параллельно с ограничением через семафор.
// Использует errgroup для управления ошибками и отменой.
func (c *Client) ForEachRow(
	ctx context.Context,
	scanRows func(pgx.Rows) error,
	sql string,
	args ...any,
) error {
	// Группируем бакеты по шардам
	groups := make(map[string][]BucketID) // shard -> []bucketID
	for bucket := range c.config.Buckets {
		shard, err := c.mapping.GetShard(BucketID(bucket))
		if err != nil {
			return fmt.Errorf("get shard for bucket %d: %w", bucket, err)
		}
		groups[shard] = append(groups[shard], BucketID(bucket))
	}
	if len(groups) == 0 {
		return fmt.Errorf("no shards found")
	}

	// Создаём errgroup с отменяемым контекстом
	eg, ctx := errgroup.WithContext(ctx)

	// Ограничиваем параллелизм числом шардов, но не больше maxParallel
	limit := min(c.maxParallel, len(groups))
	eg.SetLimit(limit)

	for shard, buckets := range groups {
		eg.Go(func() error {
			pool, err := c.wrapPool.GetPool(shard)
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
				schema := c.config.SchemaPrefix + strconv.Itoa(bucket.Int())
				sqlWithSchema := c.replaceSchema(sql, schema)
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
					if err := scanRows(rows); err != nil {
						rows.Close()
						return fmt.Errorf("handle error on shard %s, bucket %d: %w", shard, buckets[i], err)
					}
				}
				rows.Close()
				if err := rows.Err(); err != nil {
					return fmt.Errorf("shard %s, bucket %d: %w", shard, buckets[i], err)
				}
			}
			return nil
		})
	}

	// Ждём завершения всех горутин. При первой ошибке errgroup отменяет контекст.
	return eg.Wait()
}
