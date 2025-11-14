-- =============================================================================
-- Миграция 002: Создание справочных таблиц (dictionaries)
-- Описание: Нормализованные справочники для регионов, муниципалитетов,
--           населенных пунктов, показателей и источников данных
-- Дата: 2025-10-24
-- =============================================================================

SET search_path TO sberindex, public;

-- =============================================================================
-- 1. Справочник регионов РФ
-- =============================================================================
CREATE TABLE IF NOT EXISTS dict_regions (
    region_id SERIAL PRIMARY KEY,
    region_name VARCHAR(255) NOT NULL UNIQUE,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

COMMENT ON TABLE dict_regions IS 'Справочник субъектов Российской Федерации';
COMMENT ON COLUMN dict_regions.region_id IS 'Уникальный ID региона';
COMMENT ON COLUMN dict_regions.region_name IS 'Название региона';

CREATE INDEX idx_dict_regions_name ON dict_regions(region_name);

-- =============================================================================
-- 2. Справочник муниципалитетов
-- =============================================================================
CREATE TABLE IF NOT EXISTS dict_municipalities (
    municipality_id SERIAL PRIMARY KEY,
    region_id INTEGER NOT NULL REFERENCES dict_regions(region_id),
    municipality_name VARCHAR(500) NOT NULL,
    municipality_level VARCHAR(50), -- 'upper' или 'lower'
    oktmo_code VARCHAR(50), -- код ОКТМО
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(region_id, municipality_name, municipality_level)
);

COMMENT ON TABLE dict_municipalities IS 'Справочник муниципальных образований (верхний и нижний уровень)';
COMMENT ON COLUMN dict_municipalities.municipality_level IS 'Уровень муниципалитета: upper (городской округ, муниципальный район) или lower (поселения)';
COMMENT ON COLUMN dict_municipalities.oktmo_code IS 'Код ОКТМО (Общероссийский классификатор территорий муниципальных образований)';

CREATE INDEX idx_dict_municipalities_region ON dict_municipalities(region_id);
CREATE INDEX idx_dict_municipalities_name ON dict_municipalities(municipality_name);
CREATE INDEX idx_dict_municipalities_oktmo ON dict_municipalities(oktmo_code);

-- =============================================================================
-- 3. Справочник населенных пунктов
-- =============================================================================
CREATE TABLE IF NOT EXISTS dict_settlements (
    settlement_id SERIAL PRIMARY KEY,
    region_id INTEGER NOT NULL REFERENCES dict_regions(region_id),
    municipality_up_id INTEGER REFERENCES dict_municipalities(municipality_id), -- муниципалитет верхнего уровня
    municipality_down_id INTEGER REFERENCES dict_municipalities(municipality_id), -- муниципалитет нижнего уровня
    settlement_name VARCHAR(500) NOT NULL,
    settlement_name_clean VARCHAR(500), -- название без типа
    settlement_type VARCHAR(100), -- тип: город, поселок, село, деревня и т.д.
    oktmo_code VARCHAR(50), -- код ОКТМО поселения
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(region_id, settlement_name)
);

COMMENT ON TABLE dict_settlements IS 'Справочник населенных пунктов';
COMMENT ON COLUMN dict_settlements.settlement_name IS 'Полное название с указанием типа населенного пункта';
COMMENT ON COLUMN dict_settlements.settlement_name_clean IS 'Название без типа населенного пункта';
COMMENT ON COLUMN dict_settlements.settlement_type IS 'Тип населенного пункта: город, поселок, село и др.';

CREATE INDEX idx_dict_settlements_region ON dict_settlements(region_id);
CREATE INDEX idx_dict_settlements_mun_up ON dict_settlements(municipality_up_id);
CREATE INDEX idx_dict_settlements_mun_down ON dict_settlements(municipality_down_id);
CREATE INDEX idx_dict_settlements_name ON dict_settlements(settlement_name);
CREATE INDEX idx_dict_settlements_oktmo ON dict_settlements(oktmo_code);

-- =============================================================================
-- 4. Справочник блоков данных
-- =============================================================================
CREATE TABLE IF NOT EXISTS dict_data_blocks (
    block_id SERIAL PRIMARY KEY,
    block_name VARCHAR(500) NOT NULL UNIQUE,
    block_code VARCHAR(100), -- короткий код блока
    description TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

COMMENT ON TABLE dict_data_blocks IS 'Справочник блоков данных (ИНСТРУМЕНТАЛЬНЫЕ, ESG блоки и др.)';

CREATE INDEX idx_dict_data_blocks_code ON dict_data_blocks(block_code);

-- =============================================================================
-- 5. Справочник доменов рейтинга
-- =============================================================================
CREATE TABLE IF NOT EXISTS dict_rating_domains (
    domain_id SERIAL PRIMARY KEY,
    domain_name VARCHAR(255) NOT NULL UNIQUE,
    description TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

COMMENT ON TABLE dict_rating_domains IS 'Справочник доменов рейтинга (ДОХОДЫ, ЖИЛЬЕ, ЗДОРОВЬЕ и др.)';

CREATE INDEX idx_dict_rating_domains_name ON dict_rating_domains(domain_name);

-- =============================================================================
-- 6. Справочник источников данных
-- =============================================================================
CREATE TABLE IF NOT EXISTS dict_data_sources (
    source_id SERIAL PRIMARY KEY,
    source_name VARCHAR(255) NOT NULL UNIQUE,
    source_type VARCHAR(100), -- bdpmo, form, rosstat и др.
    description TEXT,
    access_type VARCHAR(50), -- 'открытый', 'запрос'
    data_format VARCHAR(50), -- 'распространение', 'запрос'
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

COMMENT ON TABLE dict_data_sources IS 'Справочник источников данных';
COMMENT ON COLUMN dict_data_sources.access_type IS 'Тип доступа к данным: открытый или по запросу';
COMMENT ON COLUMN dict_data_sources.data_format IS 'Формат получения данных';

CREATE INDEX idx_dict_data_sources_name ON dict_data_sources(source_name);
CREATE INDEX idx_dict_data_sources_type ON dict_data_sources(source_type);

-- =============================================================================
-- 7. Справочник показателей (основной справочник из Кодбука)
-- =============================================================================
CREATE TABLE IF NOT EXISTS dict_indicators (
    indicator_id SERIAL PRIMARY KEY,
    block_id INTEGER REFERENCES dict_data_blocks(block_id),
    domain_id INTEGER REFERENCES dict_rating_domains(domain_id),
    source_id INTEGER REFERENCES dict_data_sources(source_id),
    indicator_number VARCHAR(50), -- номер показателя (1.1, 1.2, 2.1.2 и т.д.)
    indicator_name TEXT NOT NULL,
    acronym VARCHAR(255) UNIQUE, -- акроним (например, wage_average, pop_total)
    calculation_method TEXT, -- метод расчёта
    normalization_method TEXT, -- метод нормализации
    comment TEXT,
    is_active BOOLEAN DEFAULT TRUE,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(indicator_number, acronym)
);

COMMENT ON TABLE dict_indicators IS 'Справочник показателей из Кодбука';
COMMENT ON COLUMN dict_indicators.indicator_number IS 'Номер показателя (например, 1.1, 2.1.2)';
COMMENT ON COLUMN dict_indicators.acronym IS 'Акроним показателя в базе данных (имя колонки из Excel)';
COMMENT ON COLUMN dict_indicators.calculation_method IS 'Метод расчёта показателя';
COMMENT ON COLUMN dict_indicators.normalization_method IS 'Метод нормализации показателя';

CREATE INDEX idx_dict_indicators_block ON dict_indicators(block_id);
CREATE INDEX idx_dict_indicators_domain ON dict_indicators(domain_id);
CREATE INDEX idx_dict_indicators_source ON dict_indicators(source_id);
CREATE INDEX idx_dict_indicators_acronym ON dict_indicators(acronym);
CREATE INDEX idx_dict_indicators_number ON dict_indicators(indicator_number);
CREATE INDEX idx_dict_indicators_active ON dict_indicators(is_active) WHERE is_active = TRUE;

-- =============================================================================
-- 8. Справочник ESG блоков (цели устойчивого развития)
-- =============================================================================
CREATE TABLE IF NOT EXISTS dict_esg_blocks (
    esg_block_id SERIAL PRIMARY KEY,
    block_name TEXT NOT NULL UNIQUE,
    block_number INTEGER, -- порядковый номер цели (1, 2, 3...)
    description TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

COMMENT ON TABLE dict_esg_blocks IS 'Справочник ESG блоков и целей устойчивого развития ООН';
COMMENT ON COLUMN dict_esg_blocks.block_number IS 'Порядковый номер цели устойчивого развития (1-17)';

CREATE INDEX idx_dict_esg_blocks_number ON dict_esg_blocks(block_number);

-- =============================================================================
-- Конец миграции 002
-- =============================================================================
