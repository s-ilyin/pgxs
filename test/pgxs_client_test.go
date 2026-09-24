//go:build pgxs_integration

package test

import (
	"errors"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/s-ilyin/pgxs"
	"github.com/stretchr/testify/require"
)

var ErrTestError = errors.New("test error")

// testUser — структура для тестов
type testUser struct {
	ID   KeyShardID
	Name string
	Age  int
}

type testUserWithBucketID struct {
	bucketID pgxs.BucketID
	value    testUser
}

type KeyShardID string

// реализует PreHasher
func (u KeyShardID) Prehash() []byte {
	return []byte(u)
}

type setupTest struct {
	client   *pgxs.Client
	buckets  map[pgxs.BucketID]*pgxpool.Pool
	pgShard1 *pgxpool.Pool
	pgShard2 *pgxpool.Pool
}

func setupTestClient(t *testing.T) setupTest {
	dsn1 := os.Getenv("PG_SHARD_1_DSN")
	dsn2 := os.Getenv("PG_SHARD_2_DSN")

	cfg := &pgxs.Config{
		Buckets:      4,
		SchemaPrefix: "bucket_",
		Shards: []pgxs.Shard{
			{Name: "shard_1", DSN: dsn1},
			{Name: "shard_2", DSN: dsn2},
		},
		Mapping: []pgxs.MappingEntry{
			{Bucket: 0, Shard: "shard_1"},
			{Bucket: 1, Shard: "shard_1"},
			{Bucket: 2, Shard: "shard_2"},
			{Bucket: 3, Shard: "shard_2"},
		},
	}
	s := setupTest{
		buckets: map[pgxs.BucketID]*pgxpool.Pool{},
	}

	client, err := pgxs.New(
		t.Context(), cfg,
		pgxs.WithConcurrency(4),
		pgxs.WithBatchTx(true),
	)
	require.NoError(t, err)
	s.client = client

	poolConfig, err := pgxpool.ParseConfig(dsn1)
	require.NoError(t, err)

	s.pgShard1, err = pgxpool.NewWithConfig(t.Context(), poolConfig)
	require.NoError(t, err)
	s.buckets[pgxs.BucketID(0)] = s.pgShard1
	s.buckets[pgxs.BucketID(1)] = s.pgShard1

	poolConfig, err = pgxpool.ParseConfig(dsn2)
	require.NoError(t, err)

	s.pgShard2, err = pgxpool.NewWithConfig(t.Context(), poolConfig)
	require.NoError(t, err)
	s.buckets[pgxs.BucketID(2)] = s.pgShard2
	s.buckets[pgxs.BucketID(3)] = s.pgShard2

	// Проверяем доступность
	err = s.pgShard1.Ping(t.Context())
	require.NoError(t, err)

	err = s.pgShard2.Ping(t.Context())
	require.NoError(t, err)

	return s
}

func setupTestClose(setup setupTest) {
	setup.pgShard1.Close()
	setup.pgShard2.Close()
	setup.client.Close()
}

// cleanupTestData очищает все таблицы перед тестом
func cleanupTestData(t *testing.T, setup setupTest) {
	for b, p := range setup.buckets {
		q := fmt.Sprintf("TRUNCATE TABLE bucket_%d.users CASCADE", b)
		_, err := p.Exec(t.Context(), q)
		require.NoError(t, err)
	}
}

// insertTestData вставляет тестовые данные для проверки
func insertTestData(t *testing.T, setup setupTest, testData ...testUserWithBucketID) {
	for _, td := range testData {
		query := fmt.Sprintf("INSERT INTO bucket_%d.users (id, name, age) VALUES ($1, $2, $3)", td.bucketID)
		pool := setup.buckets[td.bucketID]
		_, err := pool.Exec(t.Context(), query, td.value.ID, td.value.Name, td.value.Age)
		require.NoError(t, err)
	}
}

// --- Тесты ---

func TestClient_Exec(t *testing.T) {
	setup := setupTestClient(t)
	t.Cleanup(func() { setupTestClose(setup) })

	t.Run("Exec with key hash", func(t *testing.T) {
		defer cleanupTestData(t, setup)

		want := testUser{
			ID:   KeyShardID("user_key_1"),
			Name: "test_user",
			Age:  20,
		}

		// 1. Вставляем через клиент
		_, err := setup.client.ExecPresher(t.Context(), want.ID,
			`INSERT INTO {schema}.users (id, name, age) VALUES ($1, $2, $3)`,
			want.ID, want.Name, want.Age)
		require.NoError(t, err)

		// 2. Вычисляем bucketID ТОЧНО ТАК ЖЕ, как это делает клиент
		bucketID := pgxs.BucketID(pgxs.HashKey(want.ID.Prehash(), 4))

		// 3. Проверяем через прямой пул
		p := setup.buckets[bucketID]
		got := testUser{}
		q := fmt.Sprintf("SELECT id, name, age FROM bucket_%d.users WHERE id = $1", bucketID)
		err = p.QueryRow(t.Context(), q, want.ID).
			Scan(&got.ID, &got.Name, &got.Age)
		require.NoError(t, err)
		require.Equal(t, want, got)
	})

	t.Run("ExecBucket with ID", func(t *testing.T) {
		defer cleanupTestData(t, setup)

		want := testUser{
			ID:   KeyShardID("user_key_2"),
			Name: "test_user",
			Age:  20,
		}
		_, err := setup.client.ExecBucket(t.Context(), pgxs.BucketID(3),
			`INSERT INTO {schema}.users (id, name, age) VALUES ($1, $2, $3)`,
			want.ID, want.Name, want.Age)
		require.NoError(t, err)

		got := testUser{}

		err = setup.pgShard2.QueryRow(t.Context(), `SELECT id, name, age FROM bucket_3.users WHERE id = $1`, want.ID).
			Scan(&got.ID, &got.Name, &got.Age)
		require.NoError(t, err)
		require.Equal(t, want, got)
	})
}

func TestClient_Query(t *testing.T) {
	setup := setupTestClient(t)
	t.Cleanup(func() { setupTestClose(setup) })

	t.Run("QueryRow with key", func(t *testing.T) {
		defer cleanupTestData(t, setup)

		want := testUser{
			ID:   KeyShardID("u1_query"),
			Name: "Test",
			Age:  10,
		}

		insertTestData(t, setup, testUserWithBucketID{
			bucketID: pgxs.BucketID(pgxs.HashKey(want.ID.Prehash(), 4)),
			value:    want,
		})

		got := testUser{}

		err := setup.client.QueryRowPresher(t.Context(), want.ID,
			`SELECT id, name, age FROM {schema}.users WHERE id = $1`,
			want.ID).Scan(&got.ID, &got.Name, &got.Age)
		require.NoError(t, err)
		require.Equal(t, want, got)
	})

	t.Run("QueryRowBucket with bucketID", func(t *testing.T) {
		defer cleanupTestData(t, setup)

		want := testUser{
			ID:   KeyShardID("u1_bucket"),
			Name: "Test",
			Age:  10,
		}

		insertTestData(t, setup, testUserWithBucketID{
			bucketID: 1,
			value:    want,
		})

		got := testUser{}

		err := setup.client.QueryRowBucket(t.Context(), 1,
			`SELECT id, name, age FROM {schema}.users WHERE id = $1`,
			want.ID).Scan(&got.ID, &got.Name, &got.Age)
		require.NoError(t, err)
		require.Equal(t, want, got)
	})
}

func Test_Exec(t *testing.T) {
	setup := setupTestClient(t)
	t.Cleanup(func() { setupTestClose(setup) })

	t.Run("Exec insert", func(t *testing.T) {
		defer cleanupTestData(t, setup)

		users := []testUser{
			{ID: KeyShardID("b1"), Name: "Bulk1", Age: 10},
			{ID: KeyShardID("b2"), Name: "Bulk2", Age: 20},
			{ID: KeyShardID("b3"), Name: "Bulk3", Age: 30},
		}

		result, err := pgxs.Exec(
			t.Context(), setup.client, users,
			func(u testUser) []byte { return u.ID.Prehash() },
			func(b pgxs.BucketID, u testUser) (string, []any) {
				return `INSERT INTO {schema}.users (id, name, age) VALUES ($1, $2, $3)`,
					[]any{u.ID, u.Name, u.Age}
			},
		)
		require.NoError(t, err)
		require.Equal(t, 3, result.Total)
		require.Equal(t, int64(3), result.RowsAffected)

		var total int
		for b, p := range setup.buckets {
			var count int
			query := fmt.Sprintf("SELECT COUNT(*) FROM bucket_%d.users WHERE id IN ($1, $2, $3)", b)
			err = p.QueryRow(t.Context(), query, "b1", "b2", "b3").Scan(&count)
			require.NoError(t, err)
			total += count
		}

		require.NoError(t, err)
		require.Equal(t, 3, total)
	})
}

// Test_Query_NoRetriesForInvalidSQL проверяет, что при неповторяемой ошибке
// (например, синтаксической ошибке SQLSTATE 42601) ретраев НЕ происходит,
// даже если MaxAttempts > 1. Проверяется как с транзакцией, так и без неё.
func Test_Query_NoRetriesForInvalidSQL(t *testing.T) {
	dsn1 := os.Getenv("PG_SHARD_1_DSN")
	dsn2 := os.Getenv("PG_SHARD_2_DSN")

	// Проверяем оба режима: с транзакцией и без.
	for _, batchTx := range []bool{true, false} {
		name := "with_tx"
		if !batchTx {
			name = "without_tx"
		}
		t.Run(name, func(t *testing.T) {
			cfg := &pgxs.Config{
				Buckets:      4,
				SchemaPrefix: "bucket_",
				Shards: []pgxs.Shard{
					{Name: "shard_1", DSN: dsn1},
					{Name: "shard_2", DSN: dsn2},
				},
				Mapping: []pgxs.MappingEntry{
					{Bucket: 0, Shard: "shard_1"},
					{Bucket: 1, Shard: "shard_1"},
					{Bucket: 2, Shard: "shard_2"},
					{Bucket: 3, Shard: "shard_2"},
				},
				// Явно разрешаем 3 попытки — если ретраи ошибочно сработают,
				// счётчик queryCalls будет больше len(users).
				Retry: pgxs.RetryConfig{
					MaxAttempts: 3,
					BaseDelay:   10 * time.Millisecond,
					MaxDelay:    50 * time.Millisecond,
				},
			}

			client, err := pgxs.New(
				t.Context(), cfg,
				pgxs.WithBatchTx(batchTx),
			)
			require.NoError(t, err)
			defer client.Close()

			// Счётчик вызовов callback'а query (по одному на элемент за попытку).
			var queryCalls int

			users := []testUser{
				{ID: KeyShardID("bad1")},
				{ID: KeyShardID("bad2")},
				{ID: KeyShardID("bad3")},
			}

			_, err = pgxs.Query(
				t.Context(), client, users,
				func(u testUser) []byte { return u.ID.Prehash() },
				func(b pgxs.BucketID, u testUser) (string, []any) {
					queryCalls++
					// Заведомо невалидный SQL → SQLSTATE 42601 (syntax_error),
					// который НЕ входит в список retryable.
					return `THIS IS INVALID SQL SYNTAX !!!`, nil
				},
				func(rows pgx.Rows) (string, error) {
					var id string
					errScan := rows.Scan(&id)
					return id, errScan
				},
			)

			// Ошибка должна вернуться (валидный парсинг SQL не пройдёт).
			require.Error(t, err)

			// Ключевая проверка: queryCalls должен равняться числу элементов
			// (по одному вызову на элемент за ЕДИНСТВЕННУЮ попытку).
			// Если бы ретраи сработали, queryCalls был бы кратен len(users).
			require.Equal(t, len(users), queryCalls,
				"non-retryable syntax error must not trigger retries")
		})
	}
}

// Test_Query проверяет массовую функцию Query.
func Test_Query(t *testing.T) {
	setup := setupTestClient(t)
	t.Cleanup(func() { setupTestClose(setup) })

	t.Run("Query insert returning", func(t *testing.T) {
		defer cleanupTestData(t, setup)

		users := []testUser{
			{ID: KeyShardID("q1"), Name: "Query1", Age: 11},
			{ID: KeyShardID("q2"), Name: "Query2", Age: 22},
			{ID: KeyShardID("q3"), Name: "Query3", Age: 33},
		}

		ids, err := pgxs.Query(
			t.Context(), setup.client, users,
			func(u testUser) []byte { return u.ID.Prehash() },
			func(b pgxs.BucketID, u testUser) (string, []any) {
				return `INSERT INTO {schema}.users (id, name, age) VALUES ($1, $2, $3) RETURNING id`,
					[]any{u.ID, u.Name, u.Age}
			},
			func(rows pgx.Rows) (string, error) {
				var id string
				err := rows.Scan(&id)
				return id, err
			},
		)
		require.NoError(t, err)
		require.Len(t, ids, 3)
		require.ElementsMatch(t, []string{"q1", "q2", "q3"}, ids)

		// Проверяем, что данные действительно вставились
		var total int
		for b, p := range setup.buckets {
			var count int
			query := fmt.Sprintf("SELECT COUNT(*) FROM bucket_%d.users WHERE id IN ($1, $2, $3)", b)
			err = p.QueryRow(t.Context(), query, "q1", "q2", "q3").Scan(&count)
			require.NoError(t, err)
			total += count
		}
		require.Equal(t, 3, total)
	})

	t.Run("Query select multiple rows", func(t *testing.T) {
		defer cleanupTestData(t, setup)

		// Вставляем данные в бакеты, которые вычисляются по хешу —
		// именно туда пойдут запросы Query для этих же ID.
		type insertCase struct {
			id   string
			name string
			age  int
		}
		cases := []insertCase{
			{id: "s1", name: "Sel1", age: 100},
			{id: "s2", name: "Sel2", age: 200},
			{id: "s3", name: "Sel3", age: 300},
			{id: "s4", name: "Sel4", age: 400},
		}
		for _, c := range cases {
			bucketID := pgxs.BucketID(pgxs.HashKey([]byte(c.id), 4))
			insertTestData(t, setup, testUserWithBucketID{
				bucketID: bucketID,
				value:    testUser{ID: KeyShardID(c.id), Name: c.name, Age: c.age},
			})
		}

		users := []testUser{
			{ID: "s1"},
			{ID: "s2"},
			{ID: "s3"},
			{ID: "s4"},
		}

		names, err := pgxs.Query(
			t.Context(), setup.client, users,
			func(u testUser) []byte { return u.ID.Prehash() },
			func(b pgxs.BucketID, u testUser) (string, []any) {
				return `SELECT name FROM {schema}.users WHERE id = $1`, []any{u.ID}
			},
			func(rows pgx.Rows) (string, error) {
				var name string
				err := rows.Scan(&name)
				return name, err
			},
		)
		require.NoError(t, err)
		require.Len(t, names, 4)
		require.ElementsMatch(t, []string{"Sel1", "Sel2", "Sel3", "Sel4"}, names)
	})

	t.Run("Query update returning", func(t *testing.T) {
		defer cleanupTestData(t, setup)

		type insertCase struct {
			id   string
			name string
			age  int
		}
		cases := []insertCase{
			{id: "u1", name: "Upd1", age: 10},
			{id: "u2", name: "Upd2", age: 20},
		}
		for _, c := range cases {
			bucketID := pgxs.BucketID(pgxs.HashKey([]byte(c.id), 4))
			insertTestData(t, setup, testUserWithBucketID{
				bucketID: bucketID,
				value:    testUser{ID: KeyShardID(c.id), Name: c.name, Age: c.age},
			})
		}

		users := []testUser{
			{ID: "u1"},
			{ID: "u2"},
		}

		ids, err := pgxs.Query(
			t.Context(), setup.client, users,
			func(u testUser) []byte { return u.ID.Prehash() },
			func(b pgxs.BucketID, u testUser) (string, []any) {
				return `UPDATE {schema}.users SET age = age + 1 WHERE id = $1 RETURNING id`, []any{u.ID}
			},
			func(rows pgx.Rows) (string, error) {
				var id string
				err := rows.Scan(&id)
				return id, err
			},
		)
		require.NoError(t, err)
		require.Len(t, ids, 2)
		require.ElementsMatch(t, []string{"u1", "u2"}, ids)
	})
}

func TestClient_Transaction(t *testing.T) {
	setup := setupTestClient(t)
	t.Cleanup(func() { setupTestClose(setup) })

	t.Run("tx commit", func(t *testing.T) {
		defer cleanupTestData(t, setup)

		want := testUser{
			ID:   KeyShardID("user_commit"),
			Name: "test",
			Age:  10,
		}

		_, err := setup.client.ExecBucket(t.Context(), 0,
			`INSERT INTO {schema}.users (id, name, age) VALUES ($1, $2, $3)`,
			want.ID, want.Name, want.Age)
		require.NoError(t, err)

		tx, err := setup.client.BeginBucket(t.Context(), 0)
		require.NoError(t, err)
		defer tx.Rollback(t.Context())

		commit := testUser{
			ID:   want.ID,
			Name: "testUpdate",
			Age:  20,
		}

		_, err = tx.Exec(t.Context(),
			`UPDATE {schema}.users SET name = $1, age = $2 WHERE id = $3`,
			commit.Name, commit.Age, commit.ID)
		require.NoError(t, err)

		got := testUser{}
		err = tx.QueryRow(t.Context(),
			`SELECT id, name, age FROM {schema}.users WHERE id = $1`,
			want.ID).Scan(&got.ID, &got.Name, &got.Age)
		require.NoError(t, err)
		require.Equal(t, commit, got)

		err = tx.Commit(t.Context())
		require.NoError(t, err)

		got = testUser{}
		err = setup.client.QueryRowBucket(t.Context(), 0,
			`SELECT id, name, age FROM {schema}.users WHERE id = $1`,
			want.ID).Scan(&got.ID, &got.Name, &got.Age)
		require.NoError(t, err)
		require.Equal(t, commit, got)
	})

	t.Run("tx rollback", func(t *testing.T) {
		defer cleanupTestData(t, setup)

		want := testUser{
			ID:   KeyShardID("user_rollback"),
			Name: "test",
			Age:  10,
		}

		_, err := setup.client.ExecBucket(t.Context(), 0,
			`INSERT INTO {schema}.users (id, name, age) VALUES ($1, $2, $3)`,
			want.ID, want.Name, want.Age)
		require.NoError(t, err)

		tx, err := setup.client.BeginBucket(t.Context(), 0)
		require.NoError(t, err)
		defer tx.Rollback(t.Context())

		rollback := testUser{
			ID:   want.ID,
			Name: "testUpdate",
			Age:  20,
		}

		_, err = tx.Exec(t.Context(),
			`UPDATE {schema}.users SET name = $1, age = $2 WHERE id = $3`,
			rollback.Name, rollback.Age, rollback.ID)
		require.NoError(t, err)

		got := testUser{}
		err = tx.QueryRow(t.Context(),
			`SELECT id, name, age FROM {schema}.users WHERE id = $1`,
			want.ID).Scan(&got.ID, &got.Name, &got.Age)
		require.NoError(t, err)
		require.Equal(t, rollback, got)

		err = tx.Rollback(t.Context())
		require.NoError(t, err)

		got = testUser{}
		err = setup.client.QueryRowBucket(t.Context(), 0,
			`SELECT id, name, age FROM {schema}.users WHERE id = $1`,
			want.ID).Scan(&got.ID, &got.Name, &got.Age)
		require.NoError(t, err)
		require.Equal(t, want, got)
	})

	t.Run("tx isolation between buckets inner one shard", func(t *testing.T) {
		defer cleanupTestData(t, setup)

		wantBucket0 := testUser{
			ID:   KeyShardID("iso1"),
			Name: "Iso1",
			Age:  10,
		}

		_, err := setup.client.ExecBucket(t.Context(), 0,
			`INSERT INTO {schema}.users (id, name, age) VALUES ($1, $2, $3)`,
			wantBucket0.ID, wantBucket0.Name, wantBucket0.Age)
		require.NoError(t, err)

		wantBucket1 := testUser{
			ID:   KeyShardID("iso2"),
			Name: "Iso2",
			Age:  20,
		}

		_, err = setup.client.ExecBucket(t.Context(), 1,
			`INSERT INTO {schema}.users (id, name, age) VALUES ($1, $2, $3)`,
			wantBucket1.ID, wantBucket1.Name, wantBucket1.Age)
		require.NoError(t, err)

		tx, err := setup.client.BeginBucket(t.Context(), 0)
		require.NoError(t, err)
		defer tx.Rollback(t.Context())

		commit := testUser{
			ID:   wantBucket1.ID,
			Name: "Iso2Update",
		}
		_, err = tx.Exec(t.Context(),
			`UPDATE {schema}.users SET name = $1 WHERE id = $2`,
			commit.Name, commit.ID)
		require.NoError(t, err)

		err = tx.Commit(t.Context())
		require.NoError(t, err)

		// Проверяем, что в bucket 0 данные не изменились
		got := testUser{}
		err = setup.client.QueryRowBucket(t.Context(), 0,
			`SELECT id, name, age FROM {schema}.users WHERE id = $1`,
			wantBucket0.ID).Scan(&got.ID, &got.Name, &got.Age)
		require.NoError(t, err)
		require.Equal(t, wantBucket0, got)

		// Проверяем, что в bucket 1 данные не изменились
		got = testUser{}
		err = setup.client.QueryRowBucket(t.Context(), 1,
			`SELECT id, name, age FROM {schema}.users WHERE id = $1`,
			wantBucket1.ID).Scan(&got.ID, &got.Name, &got.Age)
		require.NoError(t, err)
		require.Equal(t, wantBucket1, got)
	})
}

func TestClient_ForEachRow(t *testing.T) {
	setup := setupTestClient(t)
	t.Cleanup(func() { setupTestClose(setup) })

	cleanupTestData(t, setup)

	insertTestData(
		t,
		setup,
		[]testUserWithBucketID{
			{bucketID: 0, value: testUser{ID: "1", Name: "1", Age: 10}},
			{bucketID: 0, value: testUser{ID: "2", Name: "2", Age: 20}},
			{bucketID: 1, value: testUser{ID: "3", Name: "3", Age: 30}},
			{bucketID: 1, value: testUser{ID: "4", Name: "4", Age: 40}},
			{bucketID: 2, value: testUser{ID: "5", Name: "5", Age: 50}},
			{bucketID: 2, value: testUser{ID: "6", Name: "6", Age: 60}},
			{bucketID: 3, value: testUser{ID: "7", Name: "7", Age: 70}},
			{bucketID: 3, value: testUser{ID: "8", Name: "8", Age: 80}},
		}...,
	)

	t.Run("ForEachRow all rows", func(t *testing.T) {
		ids, err := pgxs.ForEachRow(
			t.Context(), setup.client,
			func(rows pgx.Rows) (string, error) {
				var id string
				err := rows.Scan(&id)
				return id, err
			},
			`SELECT id FROM {schema}.users ORDER BY id`,
		)
		require.NoError(t, err)

		expected := []string{"1", "2", "3", "4", "5", "6", "7", "8"}
		require.ElementsMatch(t, expected, ids)
	})

	t.Run("ForEachRow with filter", func(t *testing.T) {
		names, err := pgxs.ForEachRow(
			t.Context(), setup.client,
			func(rows pgx.Rows) (string, error) {
				var name string
				err := rows.Scan(&name)
				return name, err
			},
			`SELECT name FROM {schema}.users WHERE age > 30 ORDER BY name`,
		)
		require.NoError(t, err)

		expected := []string{"4", "5", "6", "7", "8"}
		require.ElementsMatch(t, expected, names)
	})

	t.Run("ForEachRow with error in handler", func(t *testing.T) {
		_, err := pgxs.ForEachRow(
			t.Context(), setup.client,
			func(rows pgx.Rows) (string, error) {
				var id string
				if err := rows.Scan(&id); err != nil {
					return "", err
				}
				if id == "3" {
					return "", ErrTestError
				}
				return id, nil
			},
			`SELECT id FROM {schema}.users ORDER BY id`,
		)
		require.Error(t, err)
	})
}

func TestClient_ErrorHandling(t *testing.T) {
	setup := setupTestClient(t)
	t.Cleanup(func() { setupTestClose(setup) })

	t.Run("QueryRow on non-existent key", func(t *testing.T) {
		id := KeyShardID("unknown")
		var name string
		err := setup.client.QueryRowPresher(t.Context(), id,
			`SELECT name FROM {schema}.users WHERE id = $1`,
			id).Scan(&name)
		require.Error(t, err)
		require.Equal(t, pgx.ErrNoRows, err)
	})

	t.Run("Exec on invalid bucket", func(t *testing.T) {
		_, err := setup.client.ExecBucket(t.Context(), 99,
			`INSERT INTO {schema}.users (id, name) VALUES ($1, $2)`,
			"invalid", "Invalid")
		require.Error(t, err)
	})

	t.Run("Bulk with empty items", func(t *testing.T) {
		users := []testUser{}
		result, err := pgxs.Exec(
			t.Context(), setup.client, users,
			func(u testUser) []byte { return u.ID.Prehash() },
			func(b pgxs.BucketID, u testUser) (string, []any) {
				return `INSERT INTO {schema}.users (id, name) VALUES ($1, $2)`,
					[]any{u.ID, u.Name}
			},
		)
		require.NoError(t, err)
		require.Equal(t, 0, result.Total)
		require.Equal(t, int64(0), result.RowsAffected)
	})
}
