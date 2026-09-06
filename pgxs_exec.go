package pgxs

import (
	"context"
	"fmt"
	"strconv"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
)

// BulkResult результат массовой операции.
type ExecResult struct {
	BatchResults map[string]BatchResult // имя шарда -> результат (только успешные)
	RowsAffected int64                  // суммарное количество затронутых строк
	Total        int                    // общее количество выполненных запросов
}

// BatchResult результат батча на одном шарде.
type BatchResult struct {
	CommandTags []pgconn.CommandTag // теги в порядке добавления запросов
}

// Bulk выполняет массовую операцию для произвольных элементов.
// Каждый элемент преобразуется в SQL-запрос через query.
// Запросы группируются по шардам и отправляются пакетами (batch).
// Автоматически подставляется {schema} на основе bucketID элемента.
// При возникновении любой ошибки выполнение прерывается и ошибка возвращается.
func Exec[T any](
	ctx context.Context,
	client *Client,
	items []T,
	prehasher func(T) []byte,
	query func(T) (sql string, args []any),
) (*ExecResult, error) {
	type itemWithBucket struct {
		item     T
		bucketID BucketID
	}
	groups := make(map[string][]itemWithBucket)

	for _, item := range items {
		prehash := prehasher(item)
		bucketID := BucketID(HashKey(prehash, client.config.Buckets))
		shard, err := client.mapping.GetShard(bucketID)
		if err != nil {
			return nil, fmt.Errorf("bucket %d: %w", bucketID, err)
		}
		groups[shard] = append(groups[shard], itemWithBucket{item: item, bucketID: bucketID})
	}

	result := &ExecResult{
		BatchResults: make(map[string]BatchResult, len(groups)),
	}

	for shard, entries := range groups {
		pool, err := client.wrapPool.GetPool(shard)
		if err != nil {
			return nil, fmt.Errorf("get pool for shard %s: %w", shard, err)
		}

		// Получаем соединение для батча
		conn, err := pool.Acquire(ctx)
		if err != nil {
			return nil, fmt.Errorf("acquire connection for shard %s: %w", shard, err)
		}
		defer conn.Release()

		var tx pgx.Tx
		if client.batchTx {
			tx, err = conn.Begin(ctx)
			if err != nil {
				return nil, fmt.Errorf("begin tx on shard %s: %w", shard, err)
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

		// Обрабатываем результаты, возвращая ошибку при первой проблеме
		tags := make([]pgconn.CommandTag, 0, batch.Len())
		for i := range batch.Len() {
			var tag pgconn.CommandTag
			tag, err = br.Exec()
			if err != nil {
				return nil, fmt.Errorf("batch results exec on shard %s, query %d: %w", shard, i, err)
			}
			tags = append(tags, tag)
		}
		err = br.Close()
		if err != nil {
			return nil, fmt.Errorf("batch results exec close %s: %w", shard, err)
		}

		// Если транзакция была, коммитим
		if client.batchTx {
			err := tx.Commit(ctx)
			if err != nil {
				return nil, fmt.Errorf("commit tx on shard %s: %w", shard, err)
			}
		}

		// Сохраняем успешные результаты
		result.BatchResults[shard] = BatchResult{CommandTags: tags}
		for _, tag := range tags {
			result.RowsAffected += tag.RowsAffected()
		}
		result.Total += len(tags)
	}

	return result, nil
}
