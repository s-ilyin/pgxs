package pgxs

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"sync"
	"time"

	"github.com/jackc/pgx/v5"
	"golang.org/x/sync/errgroup"
)

// ---------------------------------------------------------------------------
// Query.
// ---------------------------------------------------------------------------

// Query выполняет массовые запросы с чтением данных (например, SELECT или UPDATE ... RETURNING).
// Для каждого элемента вызывается query, который должен вернуть SQL и аргументы.
// Для каждой строки результата вызывается scanRows, возвращающая значение типа R.
// Все собранные значения возвращаются в одном слайсе.
//
// Важно: scanRows НЕ должна иметь побочных эффектов — при retry (например, при 40001/40P01)
// запрос будет выполнен заново, и буфер попытки полностью сбрасывается, поэтому дублирования
// значений не произойдёт. Пользователь получает уже собранные результаты.
//
// Все шарды обрабатываются параллельно с ограничением через client.concurrency.
// Возвращает собранные результаты и ошибку (объединение ошибок по всем шардам).
// Транзакции используются атомарно на уровне шарда, если client.batchTx == true.
func Query[T any, R any](
	ctx context.Context,
	client *Client,
	src []T,
	prehasher func(T) []byte,
	query func(T) (sql string, args []any),
	scanRows func(pgx.Rows) (R, error),
) ([]R, error) {
	if scanRows == nil {
		return nil, errors.New("func scanRows is nil")
	}
	if query == nil {
		return nil, errors.New("func query is nil")
	}
	if prehasher == nil {
		return nil, errors.New("func prehasher is nil")
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
			return nil, fmt.Errorf("mapping error for bucket %d: %w", bucketID, err)
		}
		groups[shard] = append(groups[shard], rowWithBucket{row: row, bucketID: bucketID})
	}

	if len(groups) == 0 {
		return nil, nil
	}

	// errgroup для управления горутинами и ограничения параллелизма
	eg, ctx := errgroup.WithContext(ctx)
	limit := min(client.concurrency, len(groups))
	eg.SetLimit(limit)

	var (
		mu     sync.Mutex
		errs   []error
		result []R
	)

	for shard, entries := range groups {
		eg.Go(func() error {
			// Локальный буфер попытки. Изолирован между попытками внутри withRetryErr,
			// поэтому при retry дубликатов не будет.
			var shardResult []R

			shardErr := withRetryErr(ctx, client.config.Retry, func() error {
				// Сбрасываем буфер перед каждой попыткой — ключевой момент для retry.
				clear(shardResult)
				shardResult = shardResult[:0]

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

				var (
					tFirstReply time.Time
					loopErr     error
				)

				for i := range batch.Len() {
					// Проверяем отмену контекста
					if err := ctx.Err(); err != nil {
						loopErr = err
						break
					}

					rows, err := br.Query()
					if err != nil {
						loopErr = fmt.Errorf("query %d: %w", i, err)
						break
					}

					// Перебираем все строки
					for rows.Next() {
						if tFirstReply.IsZero() {
							tFirstReply = time.Now()
						}
						res, err := scanRows(rows)
						if err != nil {
							rows.Close()
							loopErr = fmt.Errorf("scanRows on query %d: %w", i, err)
							break
						}
						shardResult = append(shardResult, res)
					}
					rows.Close()
					if err := rows.Err(); err != nil && loopErr == nil {
						loopErr = fmt.Errorf("rows iteration error on query %d: %w", i, err)
					}
					if loopErr != nil {
						break
					}
				}

				// Закрываем батч
				if err := br.Close(); err != nil && loopErr == nil {
					loopErr = fmt.Errorf("batch close: %w", err)
				}

				if client.batchTx && loopErr == nil {
					if err := tx.Commit(ctx); err != nil {
						loopErr = fmt.Errorf("commit: %w", err)
					}
				}

				return loopErr
			})

			if shardErr != nil {
				mu.Lock()
				errs = append(errs, fmt.Errorf("shard %s: %w", shard, shardErr))
				mu.Unlock()
				return nil // не прерываем errgroup — другие шарды продолжают работу
			}

			// Успех: добавляем результаты шарда в общий слайс
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
