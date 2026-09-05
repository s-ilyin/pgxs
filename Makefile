# Загружаем переменные из .env
include .env
export

# Переменные
BUILD_DIR=./build
DOCKER_COMPOSE=docker compose -f $(BUILD_DIR)/docker-compose.test.yml
MIGRATION_DIR := $(PWD)/test/migrations


# DSN для тестов (берём из .env)
PG_DSN_SHARD1 := $(PG_SHARD_1_DSN)
PG_DSN_SHARD2 := $(PG_SHARD_2_DSN)

# Цели
.PHONY: help
help:
	@echo "Доступные команды:"
	@echo "  make up              - Поднять контейнеры"
	@echo "  make down            - Остановить контейнеры"
	@echo "  make migrate         - Применить миграции ко всем шардам"
	@echo "  make test            - Запустить юнит-тесты"
	@echo "  make integration-test - Запустить интеграционные тесты (включает up, migrate, test, down)"
	@echo "  make clean           - Остановить и удалить контейнеры с данными"
	@echo "  make shell-shard1    - Подключиться к psql на shard1"
	@echo "  make shell-shard2    - Подключиться к psql на shard2"

.PHONY: up
up:
	$(DOCKER_COMPOSE) up -d
	sleep 3

.PHONY: down
down:
	$(DOCKER_COMPOSE) down

.PHONY: migrate
migrate:
	@echo "Применяем миграции на shard1 (порт $(PG_SHARD_1_PORT))..."
	$(DOCKER_COMPOSE) exec -T pg-shard-1 psql -U $(PG_SHARD_1_USER) -d $(PG_SHARD_1_DB_NAME) < $(MIGRATION_DIR)/pg_shard_1.sql
	@echo "Применяем миграции на shard2 (порт $(PG_SHARD_2_PORT))..."
	$(DOCKER_COMPOSE) exec -T pg-shard-2 psql -U $(PG_SHARD_2_USER) -d $(PG_SHARD_2_DB_NAME) < $(MIGRATION_DIR)/pg_shard_2.sql
	@echo "Миграции применены"

.PHONY: test
test:
	go test -v ./...

.PHONY: integration-test
integration-test: up migrate
	@echo "Запускаем интеграционные тесты..."
	PG_DSN_SHARD1="$(PG_DSN_SHARD1)" PG_DSN_SHARD1="$(PG_DSN_SHARD2)" \
		go test -tags=pgxs_integration -v ./test/
	@echo "Интеграционные тесты завершены"
	make down

.PHONY: clean
clean:
	$(DOCKER_COMPOSE) down -v

.PHONY: shell-shard1
shell-shard1:
	$(DOCKER_COMPOSE) exec pg-shard-1 psql -U $(PG_SHARD_1_USER) -d $(PG_SHARD_1_DB_NAME)

.PHONY: shell-shard2
shell-shard2:
	$(DOCKER_COMPOSE) exec pg-shard-2 psql -U $(PG_SHARD_2_USER) -d $(PG_SHARD_2_DB_NAME)