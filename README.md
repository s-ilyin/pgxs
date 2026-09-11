# pgxs

Легковесная библиотека шардирования PostgreSQL поверх [`pgx`](https://github.com/jackc/pgx).

`pgxs` даёт прозрачный доступ к кластеру PostgreSQL, разбитому на **шарды** и **бакеты** (схемы), с типобезопасным API на дженериках, пакетными операциями через `pgx.Batch`, параллельной обработкой шардов и автоматической подстановкой имени схемы в SQL.

---

## Содержание

- [Возможности](#возможности)
- [Как это работает](#как-это-работает)
- [Установка](#установка)
- [Быстрый старт](#быстрый-старт)
- [Конфигурация](#конфигурация)
- [API](#api)
- [Транзакции](#транзакции)
- [Производительность](#производительность)
- [Рекомендации](#рекомендации)
- [Ограничения](#ограничения)

---

## Возможности

- **Фиксированные бакеты.** Число бакетов задаётся заранее и не меняется при добавлении шардов. Каждый бакет — отдельная PostgreSQL-схема.
- **Согласованное распределение.** Ключ хешируется `xxhash`, результат маппится на бакет и шард.
- **Дженерики.** `Query[T, R]`, `Exec[T]`, `ForEachRow[R]` — типобезопасный API без рефлексии.
- **Пакетные операции.** Все вставки в рамках шарда идут одним `pgx.Batch`.
- **Параллелизм по шардам.** Шарды обрабатываются параллельно через `errgroup` с ограничением `WithMaxParallelQueries`.
- **Транзакции на уровне шарда.** Опция `WithBatchTx(true)` оборачивает каждый батч в `BEGIN…COMMIT`.
- **Автоматическая подстановка схемы.** В SQL достаточно писать `{schema}`, остальное библиотека подставит сама.
- **Retry.** Повторные попытки при ошибках сериализации (`40001`, `40P01`), обрывах соединения (`08xxx`) и отменах запроса (`57014`).
- **Кастомные шарды и маппинг.** Полный контроль над распределением бакетов по шардам.

---

## Как это работает

```
              ┌────────────────────────────────────────────┐
              │                  Client                     │
              │  ┌──────────┐  ┌──────────┐  ┌──────────┐   │
   key ───►  xxhash ──►│ bucket 0 │  │ bucket 1 │ … │ bucket N │   │
              │  └────┬─────┘  └────┬─────┘  └────┬─────┘   │
              │       │              │              │         │
              │       ▼              ▼              ▼         │
              │   mapping: bucketID ──► shardName           │
              │       │              │              │         │
              │       ▼              ▼              ▼         │
              │  ┌────────┐    ┌────────┐    ┌────────┐      │
              │  │shard_1 │    │shard_2 │ …  │shard_M │      │
              │  │  pool  │    │  pool  │    │  pool  │      │
              │  └────────┘    └────────┘    └────────┘      │
              └────────────────────────────────────────────┘
```

1. Пользователь передаёт ключ (реализует `PreHasher`).
2. `xxhash` вычисляет bucketID.
3. Маппинг превращает bucketID в имя шарда.
4. Все записи одного шарда идут в **один `pgx.Batch`**.
5. Батч выполняется параллельно на всех задействованных шардах.

**Бакеты — это схемы в PostgreSQL.** Например, при `SchemaPrefix = "bucket_"` и 4 бакетах получаются схемы `bucket_0`, `bucket_1`, `bucket_2`, `bucket_3`. В SQL-шаблоне пишется `{schema}`, и библиотека подставляет нужную схему в зависимости от bucketID ключа.

---

## Установка

```bash
go get github.com/s-ilyin/pgxs
```

Требуется Go 1.25+ и `pgx/v5`.

---

## Быстрый старт

### 1. Конфигурация

```go
cfg := &pgxs.Config{
    Buckets:      4,
    SchemaPrefix: "bucket_",
    Shards: []pgxs.Shard{
        {Name: "shard_1", DSN: "postgres://user:pass@host1:5432/db"},
        {Name: "shard_2", DSN: "postgres://user:pass@host2:5432/db"},
    },
    Mapping: []pgxs.MappingEntry{
        {Bucket: 0, Shard: "shard_1"},
        {Bucket: 1, Shard: "shard_1"},
        {Bucket: 2, Shard: "shard_2"},
        {Bucket: 3, Shard: "shard_2"},
    },
}
```

### 2. Создание клиента

```go
client, err := pgxs.New(ctx, cfg,
    pgxs.WithConcurrency(4),
    pgxs.WithBatchTx(true),
)
if err != nil {
    log.Fatal(err)
}
defer client.Close()
```

### 3. Определение модели

Модель должна реализовать `PreHasher`:

```go
type UserID string

func (u UserID) PreHash() []byte { return []byte(u) }

type User struct {
    ID   UserID
    Name string
    Age  int
}
```

### 4. Массовая вставка

```go
users := []User{
    {ID: "user_1", Name: "Alice", Age: 30},
    {ID: "user_2", Name: "Bob", Age: 25},
    // …
}

results, err := pgxs.Query(ctx, client, users,
    func(u User) []byte { return u.ID.PreHash() },
    func(u User) (string, []any) {
        return `INSERT INTO {schema}.users (id, name, age)
                VALUES ($1, $2, $3) RETURNING id`,
            []any{u.ID, u.Name, u.Age}
    },
    func(rows pgx.Rows) (string, error) {
        var id string
        err := rows.Scan(&id)
        return id, err
    },
)
```

### 5. Одиночные операции

```go
// Вставка
tag, err := client.Exec(ctx, userID,
    `INSERT INTO {schema}.users (id, name) VALUES ($1, $2)`,
    userID, "Alice")

// Выборка
rows, err := client.Query(ctx, userID,
    `SELECT id FROM {schema}.users WHERE id = $1`, userID)

// Одна строка
row := client.QueryRow(ctx, userID,
    `SELECT name FROM {schema}.users WHERE id = $1`, userID)
```

### 6. Запрос ко всем бакетам

```go
ids, err := pgxs.ForEachRow(ctx, client,
    func(rows pgx.Rows) (string, error) {
        var id string
        err := rows.Scan(&id)
        return id, err
    },
    `SELECT id FROM {schema}.users WHERE age > $1`,
    18,
)
```

---

## Конфигурация

### `Config`

| Поле | Описание |
|---|---|
| `Buckets` | Число виртуальных бакетов (схем). Не меняется при добавлении шардов. |
| `SchemaPrefix` | Префикс имени схемы. По умолчанию `bucket_`. |
| `Shards` | Список шардов: имя + DSN. |
| `Mapping` | Соответствие `BucketID → shardName`. Должно покрывать все бакеты. |
| `Retry` | Настройки повторных попыток. |

### Опции клиента

| Опция | Описание |
|---|---|
| `WithConcurrency(n)` | Максимум одновременных шардов. |
| `WithBatchTx(enabled)` | Оборачивать каждый батч в транзакцию. По умолчанию `true`. |
| `WithSchemaReplacer(fn)` | Кастомная функция подстановки `{schema}`. |
| `WithPoolOptions(opts...)` | Настройки пула `pgxpool`. |

### Опции пула

| Опция | Описание |
|---|---|
| `WithPoolMaxConns(n)` | Максимум соединений в пуле. |
| `WithPoolMinConns(n)` | Минимум соединений. |
| `WithPoolMaxConnIdleTime(d)` | Время жизни неактивного соединения. |
| `WithPoolHealthCheckPeriod(d)` | Интервал проверки здоровья. |
| `WithPoolMaxConnLifetime(d)` | Максимальное время жизни соединения. |

---

## API

### Массовые операции

```go
// SELECT / UPDATE ... RETURNING с чтением строк
func Query[T, R any](
    ctx context.Context,
    client *Client,
    src []T,
    prehasher func(T) []byte,
    query func(T) (sql string, args []any),
    scanRows func(pgx.Rows) (R, error),
) ([]R, error)

// INSERT / UPDATE / DELETE без чтения строк
func Exec[T any](
    ctx context.Context,
    client *Client,
    rows []T,
    prehasher func(T) []byte,
    query func(T) (sql string, args []any),
) (*ExecResult, error)

// Один SQL на все бакеты
func ForEachRow[R any](
    ctx context.Context,
    client *Client,
    scanRows func(pgx.Rows) (R, error),
    sql string,
    args ...any,
) ([]R, error)
```

### Рекомендация: делай `Exec/Query` идемпотентным при retry

Используй `ON CONFLICT DO NOTHING` (или `ON CONFLICT ... DO UPDATE`) в `INSERT`-запросах внутри `Exec`. Тогда повторный проход батча не создаст дубликатов или ошибок на уникальные ключи.

**Не идемпотентно — дубли при retry:**

```go
results, err := pgxs.Exec(ctx, client, users,
    func(u User) []byte { return u.ID.PreHash() },
    func(u User) (string, []any) {
        return `INSERT INTO {schema}.users (id, name, age)
                VALUES ($1, $2, $3)`,
            []any{u.ID, u.Name, u.Age}
    },
)
```

Если retry сработает после частичного применения — часть пользователей окажется в таблице дважды или завершиться с ошибкой на уникальный ключ.

**Идемпотентно — безопасно при retry:**

```go
results, err := pgxs.Exec(ctx, client, users,
    func(u User) []byte { return u.ID.PreHash() },
    func(u User) (string, []any) {
        return `INSERT INTO {schema}.users (id, name, age)
                VALUES ($1, $2, $3)
                ON CONFLICT (id) DO NOTHING`,
            []any{u.ID, u.Name, u.Age}
    },
)
```

### Одиночные операции

```go
func (c *Client) ExecBucket(ctx, bucketID, sql, args...) (pgconn.CommandTag, error)
func (c *Client) QueryBucket(ctx, bucketID, sql, args...) (pgx.Rows, error)
func (c *Client) QueryRowBucket(ctx, bucketID, sql, args...) pgx.Row

func (c *Client) Exec(ctx, key PreHasher, sql, args...) (pgconn.CommandTag, error)
func (c *Client) Query(ctx, key PreHasher, sql, args...) (pgx.Rows, error)
func (c *Client) QueryRow(ctx, key PreHasher, sql, args...) pgx.Row
```

### Транзакции

```go
func (c *Client) BeginBucket(ctx, bucketID) (Tx, error)
func (c *Client) Begin(ctx, key PreHasher) (Tx, error)

func (c *Client) BeginTxBucket(ctx, key bucketID, pgx.TxOptions) (Tx, error)
func (c *Client) BeginTx(ctx, key PreHasher, pgx.TxOptions) (Tx, error)

type Tx interface {
    Exec(ctx, sql, args...) (pgconn.CommandTag, error)
    Query(ctx, sql, args...) (pgx.Rows, error)
    QueryRow(ctx, sql, args...) pgx.Row
    Commit(ctx) error
    Rollback(ctx) error
}
```

Транзакция привязана к одному бакету. Схема подставляется автоматически.

---

## Транзакции

Опция `WithBatchTx(true)` (по умолчанию) оборачивает каждый батч шарда в `BEGIN…COMMIT`.

### Что это даёт

- **Атомарность на уровне шарда.** Либо все записи шарда применяются, либо ни одна.
- **Более эффективная работа сервера.** PostgreSQL обрабатывает один явный `COMMIT` быстрее, чем эквивалентное число неявных коммитов через `Sync`.
- **Плюс 10–20% пропускной способности** на больших батчах (см. [Производительность](#производительность)).

### Когда отключать

`WithBatchTx(false)` оправдан только если:

- Батч на каждый шард очень маленький (менее 500 записей).
- Атомарность на уровне шарда не требуется.

Правило выбора:

```
per_shard_n = n / shards

per_shard_n < 500  → WithBatchTx(false)
per_shard_n ≥ 500  → WithBatchTx(true)
```

---

## Производительность

Замеры на Apple M2: 2 шарда, 4 бакета, `synchronous_commit=on`, локальный Docker.

### Пропускная способность

| Размер батча `n` | TxOn (items/s) | TxOff (items/s) |
|---:|---:|---:|
| 100 | 63 065 | 62 454 |
| 500 | 104 082 | 98 457 |
| 1 000 | 134 321 | 144 394 |
| 2 500 | 143 296 | 162 212 |
| 5 000 | 170 889 | 155 990 |
| **6 500** | **185 974** | 166 767 |
| 8 000 | 180 537 | 112 741 |
| 10 000 | 170 375 | 125 905 |

### Время на одну вставку

На плато (n=5000–6500) — **5.3–5.5 µs**.

### Память и аллокации

| n | B/op | allocs/op | MB на батч |
|---:|---:|---:|---:|
| 1 000 | 1 742 334 | 24 898 | 1.7 |
| 2 500 | 4 427 993 | 62 419 | 4.4 |
| 5 000 | 9 295 787 | 124 935 | 9.3 |
| 6 500 | 12 049 437 | 162 444 | 12.0 |
| 10 000 | 18 977 468 | 249 956 | 19.0 |

Стабильно **~25 аллокаций** и **~1.9 МБ** на 1000 записей.

### Плато

- Оптимум: **n = 5000–6500**.
- Дальше (n ≥ 8000) начинается деградация из-за роста памяти и нагрузки на GC.
- После n=6500 `TxOff` падает быстрее `TxOn` — на n=8000 разрыв в 1.6 раза.

---

## Рекомендации

### 1. Размер батча

```
n = per_shard_n × shards
```

| `per_shard_n` | Назначение | Latency на шард |
|---:|---|---:|
| 500 | интерактивные запросы | ~4 мс |
| 1 000 | **рекомендация** | ~6 мс |
| 2 500 | высокий throughput | ~14 мс |
| 5 000 | максимум throughput | ~28 мс |

Для 2 шардов это даёт `n = 1000, 2000, 5000, 10000` соответственно. На практике **n=1000–2500** даёт лучший баланс.

### 2. Число шардов

- **Равномерное распределение бакетов обязательно.** `buckets % shards == 0` и `buckets ≥ shards`.
- Если `buckets < shards`, часть шардов останется без данных и не даст параллелизма.

### 3. Параллелизм

```go
pgxs.WithConcurrency(4)
```

---

## Ограничения

- **Транзакция — на один бакет.** Кросс-бакетных транзакций нет. Атомарность между бакетами обеспечивается на уровне приложения.
- **Retry и идемпотентность.** `scanRows` и `query` должны быть без побочных эффектов — при retry батч выполняется заново.
- **Плейсхолдер `{schema}` обязателен**, если SQL должен попадать в нужную схему. Без него запрос пойдёт в `search_path` по умолчанию.
- **Mapping покрывает все бакеты.** `Config.Validate` вернёт ошибку, если хотя бы один бакет не замаплен.
- **Имена шардов уникальны.** Дубликаты вызовут ошибку валидации.
- **Бакеты не переносятся между шардами на лету.** Изменение маппинга требует пересоздания клиента и переливки данных.
- **Одновременно активных шардов не больше `WithConcurrency`.** При 16 шардах и дефолте 4 — библиотека обработает их в 4 прохода.

---
