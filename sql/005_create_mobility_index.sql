-- ============================================================================
-- Миграция 005: Таблица mobility_index_municipality
-- Описание: Индекс мобильности по муниципальным образованиям
-- Автор: Claude Code
-- Дата: 2025-11-10
-- ============================================================================

-- Таблица для хранения индекса мобильности
CREATE TABLE IF NOT EXISTS sberindex.mobility_index_municipality (
    mobility_id SERIAL PRIMARY KEY,
    territory_id VARCHAR(50) NOT NULL,
    year INTEGER NOT NULL,
    municipal_district_name VARCHAR(255),
    mobility_index_km NUMERIC(10,6),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT uq_mobility_territory_year UNIQUE (territory_id, year)
);

-- Комментарии к таблице и колонкам
COMMENT ON TABLE sberindex.mobility_index_municipality IS
'Индекс мобильности населения по муниципальным образованиям РФ. Данные за 2024-2025 гг.';

COMMENT ON COLUMN sberindex.mobility_index_municipality.mobility_id IS
'Первичный ключ (автоинкремент)';

COMMENT ON COLUMN sberindex.mobility_index_municipality.territory_id IS
'ID территории (муниципального образования). Связь с consumption_municipality и market_access_municipality';

COMMENT ON COLUMN sberindex.mobility_index_municipality.year IS
'Год измерения индекса';

COMMENT ON COLUMN sberindex.mobility_index_municipality.municipal_district_name IS
'Название муниципального округа/района';

COMMENT ON COLUMN sberindex.mobility_index_municipality.mobility_index_km IS
'Индекс мобильности в километрах. Среднее расстояние перемещения населения';

-- Индексы для ускорения запросов
CREATE INDEX IF NOT EXISTS idx_mobility_territory
    ON sberindex.mobility_index_municipality(territory_id);

CREATE INDEX IF NOT EXISTS idx_mobility_year
    ON sberindex.mobility_index_municipality(year);

CREATE INDEX IF NOT EXISTS idx_mobility_territory_year
    ON sberindex.mobility_index_municipality(territory_id, year);

-- Вывод результата
DO $$
BEGIN
    RAISE NOTICE '✅ Таблица sberindex.mobility_index_municipality создана';
    RAISE NOTICE '✅ Индексы созданы: idx_mobility_territory, idx_mobility_year, idx_mobility_territory_year';
    RAISE NOTICE '📊 Готово к загрузке данных из mobility_index.xlsx';
END $$;
