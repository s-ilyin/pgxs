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

// Query выполняет массовые запросы с чтением данных (например, SELECT или UPDATE ... RETURNING).
// Для каждого элемента вызывается query, который должен вернуть SQL и аргументы.
// Затем для каждого запроса вызывается scanRows для обработки каждой строки результата.
// Все шарды обрабатываются параллельно с ограничением через client.concurrency.
// Возвращает ошибку, если хотя бы один запрос завершился ошибкой (объединяет все ошибки).
// Транзакции используются атомарно на уровне шарда, если client.batchTx == true.
func Query[T any](
	ctx context.Context,
	client *Client,
	src []T,
	prehasher func(T) []byte,
	query func(T) (sql string, args []any),
	scanRows func(pgx.Rows) error,
) error {
	if scanRows == nil {
		return errors.New("func scanRows is nil")
	}
	if query == nil {
		return errors.New("func query is nil")
	}
	if prehasher == nil {
		return errors.New("func prehasher is nil")
	}

	type rowWithBucket struct {
		row      T
		bucketID BucketID
	}
	groups := make(map[string][]rowWithBucket)

	for _, row := range src {
		prehash := prehasher(row)
		bucketID := BucketIdFromHash(prehash, client.config.Buckets)
		shard, err := client.mapping.GetShard(bucketID)
		if err != nil {
			return fmt.Errorf("mapping error for bucket %d: %w", bucketID, err)
		}
		groups[shard] = append(groups[shard], rowWithBucket{row: row, bucketID: bucketID})
	}

	if len(groups) == 0 {
		return nil
	}

	// errgroup для управления горутинами и ограничения параллелизма
	eg, ctx := errgroup.WithContext(ctx)
	limit := min(client.concurrency, len(groups))
	eg.SetLimit(limit)

	var mu sync.Mutex
	var errs []error

	for shard, entries := range groups {
		eg.Go(func() error {
			// Оборачиваем всю операцию на шарде в withRetryErr
			shardErr := withRetryErr(ctx, client.config.Retry, func() error {
				pool, err := client.wrapPool.GetPool(shard)
				if err != nil {
					return fmt.Errorf("get pool: %w", err)
				}
				conn, err := pool.Acquire(ctx)
				if err != nil {
					return fmt.Errorf("acquire: %w", err)
				}
				defer conn.Release()

				var tx pgx.Tx
				if client.batchTx {
					tx, err = conn.Begin(ctx)
					if err != nil {
						return fmt.Errorf("begin tx: %w", err)
					}
					defer tx.Rollback(ctx)
				}

				batch := &pgx.Batch{}
				for _, entry := range entries {
					sql, args := query(entry.row)
					schema := client.config.SchemaPrefix + strconv.Itoa(entry.bucketID.Int())
					sql = client.replaceSchema(sql, schema)
					batch.Queue(sql, args...)
				}

				var br pgx.BatchResults
				if client.batchTx {
					br = tx.SendBatch(ctx, batch)
				} else {
					br = conn.SendBatch(ctx, batch)
				}
				defer br.Close()

				var shardErr error

				for i := range batch.Len() {
					// Проверяем отмену контекста
					err := ctx.Err()
					if err != nil {
						shardErr = err
						break
					}

					rows, err := br.Query()
					if err != nil {
						shardErr = fmt.Errorf("query %d: %w", i, err)
						break
					}
					defer rows.Close()

					// Перебираем все строки
					for rows.Next() {
						if err := scanRows(rows); err != nil {
							shardErr = fmt.Errorf("scanRows on query %d: %w", i, err)
							break
						}
					}
					rows.Close()
					if err := rows.Err(); err != nil && shardErr == nil {
						shardErr = fmt.Errorf("rows iteration error on query %d: %w", i, err)
					}
					if shardErr != nil {
						break
					}
				}

				// Закрываем батч
				if err := br.Close(); err != nil && shardErr == nil {
					shardErr = fmt.Errorf("batch close: %w", err)
				}

				if client.batchTx && shardErr == nil {
					if err := tx.Commit(ctx); err != nil {
						shardErr = fmt.Errorf("commit: %w", err)
					}
				}

				if shardErr != nil {
					return shardErr
				}
				return nil
			})

			if shardErr != nil {
				mu.Lock()
				errs = append(errs, fmt.Errorf("shard %s: %w", shard, shardErr))
				mu.Unlock()
			}
			return nil // не прерываем errgroup
		})
	}

	_ = eg.Wait()

	if len(errs) > 0 {
		return errors.Join(errs...)
	}
	return nil
}
