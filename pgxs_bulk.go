package pgxs

import (
	"context"
	"fmt"
	"strconv"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
)

// BulkResult результат массовой операции.
type BulkResult struct {
	ShardResults      map[string]ShardBatchResult // имя шарда -> результат
	TotalQueries      int                         // общее количество выполненных запросов
	TotalRowsAffected int64                       // суммарное количество затронутых строк
}

// ShardBatchResult результат батча на одном шарде.
type ShardBatchResult struct {
	CommandTags []pgconn.CommandTag // теги в порядке добавления запросов
	Err         error               // ошибка выполнения батча (если есть)
}

// Bulk выполняет массовую операцию для произвольных элементов.
// Каждый элемент преобразуется в SQL-запрос через query.
// Запросы группируются по шардам и отправляются пакетами (batch).
// Автоматически подставляется {schema} на основе bucketID элемента.
func Bulk[T any](
	ctx context.Context,
	client *Client,
	items []T,
	keyShard func(T) []byte,
	query func(T) (sql string, args []any),
) (*BulkResult, error) {
	type itemWithBucket struct {
		item     T
		bucketID BucketID
	}
	groups := make(map[string][]itemWithBucket)

	for _, item := range items {
		key := keyShard(item)
		bucketID := BucketID(HashKey(key, client.config.Buckets))
		shard, err := client.mapping.GetShard(bucketID)
		if err != nil {
			return nil, fmt.Errorf("bucket %d: %w", bucketID, err)
		}
		groups[shard] = append(groups[shard], itemWithBucket{item: item, bucketID: bucketID})
	}

	result := &BulkResult{
		ShardResults: make(map[string]ShardBatchResult, len(groups)),
	}

	for shard, entries := range groups {
		pool, err := client.wrapPool.GetPool(shard)
		if err != nil {
			result.ShardResults[shard] = ShardBatchResult{Err: err}
			continue
		}

		// Получаем соединение для батча
		conn, err := pool.Acquire(ctx)
		if err != nil {
			result.ShardResults[shard] = ShardBatchResult{Err: err}
			continue
		}
		defer conn.Release()

		var tx pgx.Tx
		if client.batchTx {
			tx, err = conn.Begin(ctx)
			if err != nil {
				result.ShardResults[shard] = ShardBatchResult{Err: err}
				continue
			}
			defer tx.Rollback(ctx)
		}

		batch := &pgx.Batch{}
		for _, entry := range entries {
			sql, args := query(entry.item)
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
		for range batch.Len() {
			tag, err := br.Exec()
			if err != nil {
				batchErr = err
				break
			}
			tags = append(tags, tag)
		}
		if batchErr != nil {
			result.ShardResults[shard] = ShardBatchResult{Err: batchErr}
			continue
		}

		if client.batchTx {
			err := tx.Commit(ctx)
			if err != nil {
				result.ShardResults[shard] = ShardBatchResult{Err: err}
				continue
			}
		}

		result.ShardResults[shard] = ShardBatchResult{CommandTags: tags}
		for _, tag := range tags {
			result.TotalRowsAffected += tag.RowsAffected()
		}
		result.TotalQueries += len(tags)
	}

	return result, nil
}
