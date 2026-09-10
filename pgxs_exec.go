package pgxs

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"sync"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"golang.org/x/sync/errgroup"
)

// ExecResult результат массовой операции.
type ExecResult struct {
	BatchResults BatchResults // имя шарда -> результат (только успешные)
	RowsAffected int64        // суммарное количество затронутых строк
	Total        int          // общее количество выполненных запросов
}

func (e ExecResult) Err() error {
	return e.BatchResults.Err()
}

// BatchResults — карта результатов по шардам с методом Err().
type BatchResults map[string]BatchResult

// Err возвращает объединённую ошибку, если хотя бы один шард завершился с ошибкой.
func (br BatchResults) Err() error {
	var err error
	for k := range br {
		if br[k].Err != nil {
			err = errors.Join(err, br[k].Err)
		}
	}
	return err
}

// BatchResult результат батча на одном шарде.
type BatchResult struct {
	CommandTags []pgconn.CommandTag // теги в порядке добавления запросов
	Err         error
}

// Exec выполняет массовую операцию для произвольных элементов.
// Каждый элемент преобразуется в SQL-запрос через query.
// Запросы группируются по шардам и отправляются пакетами (batch).
// Автоматически подставляется {schema} на основе bucketID элемента.
// Все шарды обрабатываются параллельно с ограничением maxParallel через errgroup.SetLimit.
// Возвращает ExecResult и ошибку (только для фатальных ошибок, например, ошибка маппинга).
// Ошибки на уровне шардов доступны через result.BatchResults.Err().
func Exec[T any](
	ctx context.Context,
	client *Client,
	rows []T,
	prehasher func(T) []byte,
	query func(T) (sql string, args []any),
) (*ExecResult, error) {
	type rowWithBucket struct {
		row      T
		bucketID BucketID
	}
	groups := make(map[string][]rowWithBucket)

	for _, row := range rows {
		prehash := prehasher(row)
		bucketID := BucketIdFromHash(prehash, client.config.Buckets)
		shard, err := client.mapping.GetShard(bucketID)
		if err != nil {
			return nil, fmt.Errorf("mapping error for bucket %d: %w", bucketID, err)
		}
		groups[shard] = append(groups[shard], rowWithBucket{row: row, bucketID: bucketID})
	}

	result := &ExecResult{
		BatchResults: make(BatchResults, len(groups)),
	}

	// errgroup для управления горутинами и ограничения параллелизма
	eg, ctx := errgroup.WithContext(ctx)
	limit := min(client.concurrency, len(groups))
	eg.SetLimit(limit)

	var mu sync.Mutex

	for shard, entries := range groups {
		eg.Go(func() error {
			// Оборачиваем всю операцию на шарде в withRetryErr
			shardErr := withRetryErr(ctx, client.config.Retry, func() error {
				batchResult := BatchResult{}

				pool, err := client.wrapPool.GetPool(shard)
				if err != nil {
					batchResult.Err = fmt.Errorf("get pool: %w", err)
					mu.Lock()
					result.BatchResults[shard] = batchResult
					mu.Unlock()
					return batchResult.Err
				}

				conn, err := pool.Acquire(ctx)
				if err != nil {
					batchResult.Err = fmt.Errorf("acquire: %w", err)
					mu.Lock()
					result.BatchResults[shard] = batchResult
					mu.Unlock()
					return batchResult.Err
				}
				defer conn.Release()

				var tx pgx.Tx
				if client.batchTx {
					tx, err = conn.Begin(ctx)
					if err != nil {
						batchResult.Err = fmt.Errorf("begin tx: %w", err)
						mu.Lock()
						result.BatchResults[shard] = batchResult
						mu.Unlock()
						return batchResult.Err
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

				tags := make([]pgconn.CommandTag, 0, batch.Len())
				var batchErr error

				for i := range batch.Len() {
					tag, err := br.Exec()
					if err != nil {
						batchErr = fmt.Errorf("query %d: %w", i, err)
						break
					}
					tags = append(tags, tag)
				}

				if err := br.Close(); err != nil && batchErr == nil {
					batchErr = fmt.Errorf("batch close: %w", err)
				}

				if client.batchTx && batchErr == nil {
					if err := tx.Commit(ctx); err != nil {
						batchErr = fmt.Errorf("commit: %w", err)
					}
				}

				batchResult.CommandTags = tags
				batchResult.Err = batchErr

				mu.Lock()
				result.BatchResults[shard] = batchResult
				if batchErr == nil {
					result.Total += len(tags)
					for _, tag := range tags {
						result.RowsAffected += tag.RowsAffected()
					}
				}
				mu.Unlock()

				return batchErr // возвращаем ошибку чтобы повторить
			})
			if shardErr != nil {
				// Если after retries ошибка осталась, сохраняем её
				// Но она уже может быть сохранена в BatchResults, но если нет — добавим.
				mu.Lock()
				if _, ok := result.BatchResults[shard]; !ok {
					// Если нет результата, создаём пустой с ошибкой
					result.BatchResults[shard] = BatchResult{Err: shardErr}
				} else {
					// Если результат уже есть, но ошибка не была сохранена (может быть, если ошибка в начале)
					if result.BatchResults[shard].Err == nil {
						br := result.BatchResults[shard]
						br.Err = shardErr
						result.BatchResults[shard] = br
					}
				}
				mu.Unlock()
			}
			return nil // не прерываем errgroup
		})
	}

	// Ждём завершения всех горутин (ошибки не возвращаем, они уже в BatchResults)
	_ = eg.Wait()
	return result, nil
}
