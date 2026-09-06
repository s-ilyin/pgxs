package pgxs

import (
	"context"
	"fmt"
	"strconv"

	"github.com/jackc/pgx/v5"
)

// Query выполняет массовые запросы с чтением данных (например, с RETURNING).
// Возвращает результаты в том же порядке, что и входные элементы.
func Query[T any, R any](
	ctx context.Context,
	client *Client,
	src []T,
	prehasher func(T) []byte,
	query func(T) (sql string, args []any),
	scanRows func(pgx.Rows) error,
) error {
	type itemWithBucket struct {
		item     T
		bucketID BucketID
		index    int // для сохранения порядка
	}
	groups := make(map[string][]itemWithBucket)

	for idx, item := range src {
		prehash := prehasher(item)
		bucketID := BucketID(HashKey(prehash, client.config.Buckets))
		shard, err := client.mapping.GetShard(bucketID)
		if err != nil {
			return fmt.Errorf("bucket %d: %w", bucketID, err)
		}
		groups[shard] = append(groups[shard], itemWithBucket{
			item:     item,
			bucketID: bucketID,
			index:    idx,
		})
	}

	for shard, entries := range groups {
		pool, err := client.wrapPool.GetPool(shard)
		if err != nil {
			return fmt.Errorf("get pool for shard %s: %w", shard, err)
		}

		conn, err := pool.Acquire(ctx)
		if err != nil {
			return fmt.Errorf("acquire connection for shard %s: %w", shard, err)
		}
		defer conn.Release()

		var tx pgx.Tx
		if client.batchTx {
			tx, err = conn.Begin(ctx)
			if err != nil {
				return fmt.Errorf("begin tx on shard %s: %w", shard, err)
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

		for i := range batch.Len() {
			var rows pgx.Rows
			rows, err = br.Query()
			if err != nil {
				return fmt.Errorf("shard %s, query %d: %w", shard, i, err)
			}
			defer rows.Close()

			// Вызываем scanRows для обработки строк
			err = scanRows(rows)
			if err != nil {
				return fmt.Errorf("shard %s, query %d: %w", shard, i, err)
			}
			// Проверяем, что есть хотя бы одна строка
			if !rows.Next() {
				return fmt.Errorf("shard %s, query %d: no rows returned", shard, i)
			}
		}
		err = br.Close()
		if err != nil {
			return fmt.Errorf("batch results close on shard %s: %w", shard, err)
		}

		// Если транзакция была, коммитим
		if client.batchTx {
			err := tx.Commit(ctx)
			if err != nil {
				return fmt.Errorf("commit tx on shard %s: %w", shard, err)
			}
		}
	}

	return nil
}
