-- =============================================================================
-- Миграция 003: Создание фактовых таблиц и метаданных
-- Описание: Таблицы для хранения фактических данных показателей
--           и метаинформации о населенных пунктах
-- Дата: 2025-10-24
-- =============================================================================

SET search_path TO sberindex, public;

-- =============================================================================
-- 1. Атрибуты населенных пунктов (метаданные)
-- =============================================================================
CREATE TABLE IF NOT EXISTS meta_settlement_attributes (
    attribute_id BIGSERIAL PRIMARY KEY,
    settlement_id INTEGER NOT NULL REFERENCES dict_settlements(settlement_id) ON DELETE CASCADE,
    is_arctic BOOLEAN DEFAULT FALSE, -- входит ли в Арктическую зону
    is_remote BOOLEAN DEFAULT FALSE, -- отдаленный и труднодоступный
    is_special BOOLEAN DEFAULT FALSE, -- специальный статус (ЗАТО и др.)
    is_suburb BOOLEAN DEFAULT FALSE, -- пригород (30-40 минут от центра)
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(settlement_id)
);

COMMENT ON TABLE meta_settlement_attributes IS 'Атрибуты и характеристики населенных пунктов';
COMMENT ON COLUMN meta_settlement_attributes.is_arctic IS 'Входит ли населенный пункт в состав Арктической зоны РФ';
COMMENT ON COLUMN meta_settlement_attributes.is_remote IS 'Является ли населенный пункт отдаленным и труднодоступным';
COMMENT ON COLUMN meta_settlement_attributes.is_special IS 'Имеет ли специальный статус (ЗАТО, наукоград и др.)';
COMMENT ON COLUMN meta_settlement_attributes.is_suburb IS 'Находится ли в пределах 30-40 минут от регионального центра';

CREATE INDEX idx_meta_settlement_attrs_settlement ON meta_settlement_attributes(settlement_id);
CREATE INDEX idx_meta_settlement_attrs_arctic ON meta_settlement_attributes(is_arctic) WHERE is_arctic = TRUE;
CREATE INDEX idx_meta_settlement_attrs_remote ON meta_settlement_attributes(is_remote) WHERE is_remote = TRUE;
CREATE INDEX idx_meta_settlement_attrs_special ON meta_settlement_attributes(is_special) WHERE is_special = TRUE;

-- =============================================================================
-- 2. Координаты населенных пунктов
-- =============================================================================
CREATE TABLE IF NOT EXISTS meta_settlement_coordinates (
    coordinate_id BIGSERIAL PRIMARY KEY,
    settlement_id INTEGER NOT NULL REFERENCES dict_settlements(settlement_id) ON DELETE CASCADE,
    latitude NUMERIC(10, 7), -- широта
    longitude NUMERIC(10, 7), -- долгота
    coordinate_source VARCHAR(100), -- источник координат
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(settlement_id)
);

COMMENT ON TABLE meta_settlement_coordinates IS 'Географические координаты населенных пунктов';
COMMENT ON COLUMN meta_settlement_coordinates.latitude IS 'Широта (географическая)';
COMMENT ON COLUMN meta_settlement_coordinates.longitude IS 'Долгота (географическая)';

CREATE INDEX idx_meta_settlement_coords_settlement ON meta_settlement_coordinates(settlement_id);
CREATE INDEX idx_meta_settlement_coords_latlon ON meta_settlement_coordinates(latitude, longitude);

-- =============================================================================
-- 3. Демография населенных пунктов
-- =============================================================================
CREATE TABLE IF NOT EXISTS meta_settlement_population (
    population_id BIGSERIAL PRIMARY KEY,
    settlement_id INTEGER NOT NULL REFERENCES dict_settlements(settlement_id) ON DELETE CASCADE,
    year INTEGER, -- год данных
    population_total INTEGER, -- общая численность населения
    population_men INTEGER, -- мужчины
    population_women INTEGER, -- женщины
    data_source VARCHAR(255), -- источник данных
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(settlement_id, year)
);

COMMENT ON TABLE meta_settlement_population IS 'Демографические данные по населенным пунктам';
COMMENT ON COLUMN meta_settlement_population.year IS 'Год, к которому относятся данные';

CREATE INDEX idx_meta_settlement_pop_settlement ON meta_settlement_population(settlement_id);
CREATE INDEX idx_meta_settlement_pop_year ON meta_settlement_population(year);

-- =============================================================================
-- 4. Факт-таблица: показатели по населенным пунктам (НОРМАЛИЗОВАННАЯ)
-- =============================================================================
CREATE TABLE IF NOT EXISTS fact_settlement_indicators (
    fact_id BIGSERIAL PRIMARY KEY,
    settlement_id INTEGER NOT NULL REFERENCES dict_settlements(settlement_id) ON DELETE CASCADE,
    indicator_id INTEGER NOT NULL REFERENCES dict_indicators(indicator_id) ON DELETE RESTRICT,
    year INTEGER, -- год показателя (если применимо)
    value_numeric NUMERIC(20, 6), -- числовое значение
    value_text TEXT, -- текстовое значение (для нечисловых показателей)
    value_boolean BOOLEAN, -- булево значение (для бинарных показателей)
    is_estimated BOOLEAN DEFAULT FALSE, -- является ли значение оценочным/расчётным
    data_source VARCHAR(255), -- источник конкретного значения
    comment TEXT, -- комментарии к значению
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(settlement_id, indicator_id, year)
);

COMMENT ON TABLE fact_settlement_indicators IS 'Факт-таблица показателей по населенным пунктам (нормализованная структура)';
COMMENT ON COLUMN fact_settlement_indicators.value_numeric IS 'Числовое значение показателя';
COMMENT ON COLUMN fact_settlement_indicators.value_text IS 'Текстовое значение для нечисловых показателей';
COMMENT ON COLUMN fact_settlement_indicators.value_boolean IS 'Булево значение для бинарных показателей';
COMMENT ON COLUMN fact_settlement_indicators.is_estimated IS 'Флаг: является ли значение расчётным/оценочным';

CREATE INDEX idx_fact_indicators_settlement ON fact_settlement_indicators(settlement_id);
CREATE INDEX idx_fact_indicators_indicator ON fact_settlement_indicators(indicator_id);
CREATE INDEX idx_fact_indicators_year ON fact_settlement_indicators(year);
CREATE INDEX idx_fact_indicators_settlement_year ON fact_settlement_indicators(settlement_id, year);
CREATE INDEX idx_fact_indicators_value_numeric ON fact_settlement_indicators(value_numeric) WHERE value_numeric IS NOT NULL;

-- Композитный индекс для аналитических запросов
CREATE INDEX idx_fact_indicators_analytics ON fact_settlement_indicators(indicator_id, settlement_id, year)
    INCLUDE (value_numeric, value_boolean);

-- =============================================================================
-- 5. Таблица связи показателей с ESG блоками (many-to-many)
-- =============================================================================
CREATE TABLE IF NOT EXISTS link_indicator_esg (
    link_id SERIAL PRIMARY KEY,
    indicator_id INTEGER NOT NULL REFERENCES dict_indicators(indicator_id) ON DELETE CASCADE,
    esg_block_id INTEGER NOT NULL REFERENCES dict_esg_blocks(esg_block_id) ON DELETE CASCADE,
    weight NUMERIC(5, 4), -- вес показателя в блоке (если применимо)
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(indicator_id, esg_block_id)
);

COMMENT ON TABLE link_indicator_esg IS 'Связь показателей с ESG блоками (many-to-many)';
COMMENT ON COLUMN link_indicator_esg.weight IS 'Вес/значимость показателя в рамках ESG блока';

CREATE INDEX idx_link_indicator_esg_indicator ON link_indicator_esg(indicator_id);
CREATE INDEX idx_link_indicator_esg_block ON link_indicator_esg(esg_block_id);

-- =============================================================================
-- 6. История изменений данных (audit log) - опционально
-- =============================================================================
CREATE TABLE IF NOT EXISTS audit_settlement_indicators (
    audit_id BIGSERIAL PRIMARY KEY,
    fact_id BIGINT REFERENCES fact_settlement_indicators(fact_id) ON DELETE SET NULL,
    settlement_id INTEGER NOT NULL,
    indicator_id INTEGER NOT NULL,
    old_value_numeric NUMERIC(20, 6),
    new_value_numeric NUMERIC(20, 6),
    old_value_text TEXT,
    new_value_text TEXT,
    operation VARCHAR(10), -- INSERT, UPDATE, DELETE
    changed_by VARCHAR(255), -- пользователь/процесс
    changed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

COMMENT ON TABLE audit_settlement_indicators IS 'Журнал изменений данных показателей (audit log)';

CREATE INDEX idx_audit_fact ON audit_settlement_indicators(fact_id);
CREATE INDEX idx_audit_settlement ON audit_settlement_indicators(settlement_id);
CREATE INDEX idx_audit_indicator ON audit_settlement_indicators(indicator_id);
CREATE INDEX idx_audit_timestamp ON audit_settlement_indicators(changed_at);

-- =============================================================================
-- Функция для автоматического обновления updated_at
-- =============================================================================
CREATE OR REPLACE FUNCTION update_updated_at_column()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = CURRENT_TIMESTAMP;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

-- Применение триггеров на таблицы
CREATE TRIGGER update_dict_regions_updated_at BEFORE UPDATE ON dict_regions
    FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();

CREATE TRIGGER update_dict_municipalities_updated_at BEFORE UPDATE ON dict_municipalities
    FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();

CREATE TRIGGER update_dict_settlements_updated_at BEFORE UPDATE ON dict_settlements
    FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();

CREATE TRIGGER update_dict_indicators_updated_at BEFORE UPDATE ON dict_indicators
    FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();

CREATE TRIGGER update_meta_settlement_attrs_updated_at BEFORE UPDATE ON meta_settlement_attributes
    FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();

CREATE TRIGGER update_meta_settlement_coords_updated_at BEFORE UPDATE ON meta_settlement_coordinates
    FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();

CREATE TRIGGER update_meta_settlement_pop_updated_at BEFORE UPDATE ON meta_settlement_population
    FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();

CREATE TRIGGER update_fact_settlement_indicators_updated_at BEFORE UPDATE ON fact_settlement_indicators
    FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();

-- =============================================================================
-- Конец миграции 003
-- =============================================================================
