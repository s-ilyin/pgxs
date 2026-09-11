# Загружаем переменные из .env
include .env
export

# Переменные
BUILD_DIR=./build
PHASE_DIR := $(PWD)/metrics/manual/phase2
DOCKER_COMPOSE=docker compose -f $(BUILD_DIR)/docker-compose.test.yml
BENCH_COMPOSE=docker compose -f $(BUILD_DIR)/docker-compose.bench.yml
MIGRATION_DIR := $(PWD)/test/migrations

# DSN для тестов (берём из .env)
PG_DSN_SHARD1 := $(PG_SHARD_1_DSN)
PG_DSN_SHARD2 := $(PG_SHARD_2_DSN)

# Порты bench-контейнеров
PG_SYNC_ON_1SHARD_PORT    := 5100
PG_SYNC_OFF_1SHARD_PORT   := 5110
PG_SYNC_ON_MULTI_1_PORT   := 5120
PG_SYNC_ON_MULTI_2_PORT   := 5121
PG_SYNC_OFF_MULTI_1_PORT  := 5130
PG_SYNC_OFF_MULTI_2_PORT  := 5131

# Список всех bench-сервисов PostgreSQL
BENCH_PG_SERVICES := pg-shard-1 pg-shard-2 pg-shard-3 pg-shard-4 pg-shard-5 pg-shard-6

# ============ Основной цикл ============

.PHONY: help
help:
	@echo "Доступные команды:"
	@echo ""
	@echo "  --- Основной цикл ---"
	@echo "  make compose-up              - Поднять контейнеры"
	@echo "  make compose-down            - Остановить контейнеры"
	@echo "  make up                      - Поднять и применить миграции"
	@echo "  make migrate                 - Применить миграции ко всем шардам"
	@echo "  make test                    - Запустить юнит-тесты"
	@echo "  make integration-test        - Запустить интеграционные тесты"
	@echo "  make clean                   - Остановить и удалить контейнеры с данными"
	@echo "  make shell-shard_1           - Подключиться к psql на shard_1"
	@echo "  make shell-shard_2           - Подключиться к psql на shard_2"
	@echo ""
	@echo "  --- Бенчмарки ---"
	@echo "  make bench-up                - Поднять 6 bench-контейнеров + мониторинг"
	@echo "  make bench-down              - Остановить bench-контейнеры"
	@echo "  make bench-rs                - Перезапустить bench-контейнеры"
	@echo "  make bench-migrate           - Применить миграции к bench-контейнерам"
	@echo "  make bench                   - Полный цикл: up + migrate + 4 бенчмарка"
	@echo "  make bench-full              - То же + остановка контейнеров в конце"
	@echo "  make bench-syncon            - Только SyncOn-бенчмарки (1 и 2 шарда)"
	@echo "  make bench-syncon-2sh        - Только 2 шарда, SyncOn, TxOn/TxOff"
	@echo "  make bench-reset-stats       - Сбросить pg_stat_* на всех bench-PG"
	@echo ""
	@echo "  --- Мониторинг ---"
	@echo "  make grafana                 - Открыть Grafana (localhost:3000)"
	@echo "  make prometheus              - Открыть Prometheus (localhost:9090)"
	@echo "  make grafana-logs            - Логи Grafana"
	@echo "  make prometheus-logs         - Логи Prometheus"
	@echo "  make exporters-status        - Проверить /metrics у всех exporter'ов"
	@echo "  make monitoring-down         - Остановить только Prometheus + Grafana"
	@echo ""
	@echo "  --- psql в bench-контейнерах ---"
	@echo "  make psql-1                  - psql в pg-shard-1 (SyncOn 1 шард)"
	@echo "  make psql-2                  - psql в pg-shard-2 (SyncOff 1 шард)"
	@echo "  make psql-3                  - psql в pg-shard-3 (SyncOn 2 шарда, шард 1)"
	@echo "  make psql-4                  - psql в pg-shard-4 (SyncOn 2 шарда, шард 2)"
	@echo "  make psql-5                  - psql в pg-shard-5 (SyncOff 2 шарда, шард 1)"
	@echo "  make psql-6                  - psql в pg-shard-6 (SyncOff 2 шарда, шард 2)"

# ============ Основные цели ============

.PHONY: up
up: compose-up migrate

.PHONY: compose-up
compose-up:
	$(DOCKER_COMPOSE) up -d
	sleep 1

.PHONY: compose-down
compose-down:
	$(DOCKER_COMPOSE) down

.PHONY: migrate
migrate:
	@echo "Применяем миграции на shard_1 (порт $(PG_SHARD_1_PORT))..."
	$(DOCKER_COMPOSE) exec -T pg-shard-1 psql -U $(PG_SHARD_1_USER) -d $(PG_SHARD_1_DB_NAME) < $(MIGRATION_DIR)/pg_shard_1.sql
	@echo "Применяем миграции на shard_2 (порт $(PG_SHARD_2_PORT))..."
	$(DOCKER_COMPOSE) exec -T pg-shard-2 psql -U $(PG_SHARD_2_USER) -d $(PG_SHARD_2_DB_NAME) < $(MIGRATION_DIR)/pg_shard_2.sql
	@echo "Миграции применены"

.PHONY: test
test:
	go test -v ./...

.PHONY: integration-test
integration-test: up
	@echo "Запускаем интеграционные тесты..."
	PG_DSN_SHARD1="$(PG_DSN_SHARD1)" PG_DSN_SHARD2="$(PG_DSN_SHARD2)" \
		go test -tags=pgxs_integration -v ./test/
	@echo "Интеграционные тесты завершены"
	make compose-down

.PHONY: clean
clean:
	$(DOCKER_COMPOSE) down -v

.PHONY: shell-shard_1
shell-shard_1:
	$(DOCKER_COMPOSE) exec pg-shard-1 psql -U $(PG_SHARD_1_USER) -d $(PG_SHARD_1_DB_NAME)

.PHONY: shell-shard_2
shell-shard_2:
	$(DOCKER_COMPOSE) exec pg-shard-2 psql -U $(PG_SHARD_2_USER) -d $(PG_SHARD_2_DB_NAME)

# ============ Бенчмарки ============

.PHONY: bench-up
bench-up:
	$(BENCH_COMPOSE) up -d
	@echo "Ждём готовности PostgreSQL..."
	@for svc in $(BENCH_PG_SERVICES); do \
		until [ "$$(docker inspect -f '{{.State.Health.Status}}' $$svc 2>/dev/null)" = "healthy" ]; do \
			sleep 1; \
		done; \
		echo "  $$svc: healthy"; \
	done
	@echo "Ждём exporter'ов..."
	@sleep 3
	@echo "Bench-окружение готово. Grafana: http://localhost:3000 (admin/admin)"

.PHONY: bench-down
bench-down:
	$(BENCH_COMPOSE) down

.PHONY: bench-rs
bench-rs: bench-down bench-up bench-migrate

.PHONY: bench-clean
bench-clean:
	$(BENCH_COMPOSE) down -v

.PHONY: bench-migrate
bench-migrate:
	@echo "--- 1 шард, sync=on (pg-shard-1) ---"
	$(BENCH_COMPOSE) exec -T pg-shard-1 psql -U postgres -d testdb < $(MIGRATION_DIR)/pg_single.sql

	@echo "--- 1 шард, sync=off (pg-shard-2) ---"
	$(BENCH_COMPOSE) exec -T pg-shard-2 psql -U postgres -d testdb < $(MIGRATION_DIR)/pg_single.sql

	@echo "--- 2 шарда, sync=on (pg-shard-3) ---"
	$(BENCH_COMPOSE) exec -T pg-shard-3 psql -U postgres -d testdb < $(MIGRATION_DIR)/pg_shard_1.sql

	@echo "--- 2 шарда, sync=on (pg-shard-4) ---"
	$(BENCH_COMPOSE) exec -T pg-shard-4 psql -U postgres -d testdb < $(MIGRATION_DIR)/pg_shard_2.sql

	@echo "--- 2 шарда, sync=off (pg-shard-5) ---"
	$(BENCH_COMPOSE) exec -T pg-shard-5 psql -U postgres -d testdb < $(MIGRATION_DIR)/pg_shard_1.sql

	@echo "--- 2 шарда, sync=off (pg-shard-6) ---"
	$(BENCH_COMPOSE) exec -T pg-shard-6 psql -U postgres -d testdb < $(MIGRATION_DIR)/pg_shard_2.sql

	@echo "Все миграции применены"

.PHONY: bench-reset-stats
bench-reset-stats:
	@echo "Сбрасываем статистику pg_stat_* на всех bench-PG..."
	@for svc in $(BENCH_PG_SERVICES); do \
		echo "  $$svc"; \
		$(BENCH_COMPOSE) exec -T $$svc psql -U postgres -d testdb -c \
			"SELECT pg_stat_reset_shared('wal'); SELECT pg_stat_reset_shared('bgwriter'); SELECT pg_stat_reset_shared('archiver'); SELECT pg_stat_reset();" > /dev/null; \
	done
	@echo "Статистика сброшена"

.PHONY: bench
bench: bench-up bench-migrate
	@echo ""
	@echo "=========================================="
	@echo "Сценарий 1: 1 шард, synchronous_commit=on"
	@echo "=========================================="
	PG_SYNC_ON_1SHARD_DSN="postgres://postgres:postgres@localhost:$(PG_SYNC_ON_1SHARD_PORT)/testdb?sslmode=disable" \
		go test -tags=pgxs_integration -run=^$$ -bench='BenchmarkInsert_SyncOn_1Shard' -benchmem -count=1 ./test/

	@echo ""
	@echo "=========================================="
	@echo "Сценарий 2: 1 шард, synchronous_commit=off"
	@echo "=========================================="
	PG_SYNC_OFF_1SHARD_DSN="postgres://postgres:postgres@localhost:$(PG_SYNC_OFF_1SHARD_PORT)/testdb?sslmode=disable" \
		go test -tags=pgxs_integration -run=^$$ -bench='BenchmarkInsert_SyncOff_1Shard' -benchmem -count=1 ./test/

	@echo ""
	@echo "=========================================="
	@echo "Сценарий 3: 2 шарда, synchronous_commit=on"
	@echo "=========================================="
	PG_SYNC_ON_MULTI_1_DSN="postgres://postgres:postgres@localhost:$(PG_SYNC_ON_MULTI_1_PORT)/testdb?sslmode=disable" \
	PG_SYNC_ON_MULTI_2_DSN="postgres://postgres:postgres@localhost:$(PG_SYNC_ON_MULTI_2_PORT)/testdb?sslmode=disable" \
		go test -tags=pgxs_integration -run=^$$ -bench='BenchmarkInsert_SyncOn_2Shards' -benchmem -count=1 ./test/

	@echo ""
	@echo "=========================================="
	@echo "Сценарий 4: 2 шарда, synchronous_commit=off"
	@echo "=========================================="
	PG_SYNC_OFF_MULTI_1_DSN="postgres://postgres:postgres@localhost:$(PG_SYNC_OFF_MULTI_1_PORT)/testdb?sslmode=disable" \
	PG_SYNC_OFF_MULTI_2_DSN="postgres://postgres:postgres@localhost:$(PG_SYNC_OFF_MULTI_2_PORT)/testdb?sslmode=disable" \
		go test -tags=pgxs_integration -run=^$$ -bench='BenchmarkInsert_SyncOff_2Shards' -benchmem -count=1 ./test/

	@echo ""
	@echo "Все бенчмарки завершены"

.PHONY: bench-full
bench-full: bench
	$(BENCH_COMPOSE) down

# ============ Только SyncOn ============

.PHONY: bench-syncon-2sh
bench-syncon-2sh:
	@echo ""
	@echo "=========================================="
	@echo "2 шарда, synchronous_commit=on (TxOn + TxOff)"
	@echo "=========================================="
	PG_SYNC_ON_MULTI_1_DSN="postgres://postgres:postgres@localhost:$(PG_SYNC_ON_MULTI_1_PORT)/testdb?sslmode=disable" \
	PG_SYNC_ON_MULTI_2_DSN="postgres://postgres:postgres@localhost:$(PG_SYNC_ON_MULTI_2_PORT)/testdb?sslmode=disable" \
		go test -tags=pgxs_integration -run='^$$' \
				-bench='BenchmarkInsert_SyncOn_2ShardsTx(On|Off)' \
				-benchmem -count=1 -shuffle=on \
				./test/ 2>&1 | tee metrics/bench.txt

	@echo ""
	@echo "Бенчмарки завершены"

# ============ Мониторинг ============

.PHONY: grafana
grafana:
	@command -v open >/dev/null 2>&1 && open http://localhost:3000 || \
	command -v xdg-open >/dev/null 2>&1 && xdg-open http://localhost:3000 || \
		echo "Открой вручную: http://localhost:3000 (admin/admin)"

.PHONY: prometheus
prometheus:
	@command -v open >/dev/null 2>&1 && open http://localhost:9090 || \
	command -v xdg-open >/dev/null 2>&1 && xdg-open http://localhost:9090 || \
		echo "Открой вручную: http://localhost:9090"

.PHONY: grafana-logs
grafana-logs:
	$(BENCH_COMPOSE) logs -f grafana

.PHONY: prometheus-logs
prometheus-logs:
	$(BENCH_COMPOSE) logs -f prometheus

.PHONY: exporters-status
exporters-status:
	@echo "Проверяем exporter'ы..."
	@for port in 9181 9182 9183 9184 9185 9186; do \
		printf "  :%s -> " $$port; \
		if curl -sf http://localhost:$$port/metrics > /dev/null; then echo "OK"; else echo "FAIL"; fi; \
	done
	@echo ""
	@echo "Проверяем targets в Prometheus..."
	@curl -sf http://localhost:9090/api/v1/targets | \
		grep -o '"health":"[^"]*"' | sort | uniq -c || echo "Prometheus недоступен"

.PHONY: monitoring-down
monitoring-down:
	$(BENCH_COMPOSE) stop prometheus grafana

# ============ psql в bench-контейнерах ============

.PHONY: psql-1
psql-1:
	$(BENCH_COMPOSE) exec pg-shard-1 psql -U postgres -d testdb

.PHONY: psql-2
psql-2:
	$(BENCH_COMPOSE) exec pg-shard-2 psql -U postgres -d testdb

.PHONY: psql-3
psql-3:
	$(BENCH_COMPOSE) exec pg-shard-3 psql -U postgres -d testdb

.PHONY: psql-4
psql-4:
	$(BENCH_COMPOSE) exec pg-shard-4 psql -U postgres -d testdb

.PHONY: psql-5
psql-5:
	$(BENCH_COMPOSE) exec pg-shard-5 psql -U postgres -d testdb

.PHONY: psql-6
psql-6:
	$(BENCH_COMPOSE) exec pg-shard-6 psql -U postgres -d testdb