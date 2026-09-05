-- Создаём схемы для всех бакетов
DO $$
DECLARE
    i INT;
BEGIN
    FOR i IN 0..1 LOOP
        EXECUTE format('CREATE SCHEMA IF NOT EXISTS bucket_%s', i);
    END LOOP;
END $$;

-- Создаём таблицу users в каждой схеме
DO $$
DECLARE
    schema_name TEXT;
    schemas TEXT[] := ARRAY['bucket_0', 'bucket_1'];
BEGIN
    FOREACH schema_name IN ARRAY schemas
    LOOP
        EXECUTE format('
            CREATE TABLE IF NOT EXISTS %I.users (
                id VARCHAR(32) PRIMARY KEY,
                name TEXT NOT NULL,
                age INT
            )
        ', schema_name);
    END LOOP;
END $$;