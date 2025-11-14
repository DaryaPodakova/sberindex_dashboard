-- =============================================================================
-- Миграция 004: Создание таблиц для данных СберИндекса (consumption, market_access)
-- Описание: Таблицы для хранения потребительских расходов по МО и индекса
--           доступности рынков
-- Дата: 2025-10-24
-- =============================================================================

SET search_path TO sberindex, public;

-- =============================================================================
-- 1. Таблица потребительских расходов по муниципальным образованиям
-- =============================================================================
CREATE TABLE IF NOT EXISTS consumption_municipality (
    consumption_id BIGSERIAL PRIMARY KEY,
    territory_id VARCHAR(50) NOT NULL,
    -- Код территории (МО в постоянных границах)

    date DATE NOT NULL,
    -- Год и месяц данных

    category VARCHAR(255) NOT NULL,
    -- Категория потребительских расходов:
    -- "Продовольствие", "Здоровье", "Общественное питание",
    -- "Транспорт", "Маркетплейсы", "Все категории"

    consumption NUMERIC(15, 2),
    -- Оценка средних безналичных потребительских расходов в текущем месяце
    -- жителей МО на основе моделей СберИндекса на транзакционных данных (руб.)

    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,

    UNIQUE(territory_id, date, category)
);

COMMENT ON TABLE consumption_municipality IS 'Потребительские безналичные расходы на уровне муниципальных образований по категориям трат (январь 2023 - декабрь 2024). Источник: СберИндекс, лицензия CC BY-SA 4.0';

COMMENT ON COLUMN consumption_municipality.territory_id IS 'Код территории (муниципальное образование в постоянных границах)';
COMMENT ON COLUMN consumption_municipality.date IS 'Год и месяц данных (первое число месяца)';
COMMENT ON COLUMN consumption_municipality.category IS 'Категория потребительских расходов: Продовольствие, Здоровье, Общественное питание, Транспорт, Маркетплейсы, Все категории';
COMMENT ON COLUMN consumption_municipality.consumption IS 'Оценка средних безналичных потребительских расходов жителей МО в текущем месяце (руб.). Рассчитано по моделям СберИндекса на транзакционных данных';

-- Индексы для эффективных запросов
CREATE INDEX idx_consumption_territory ON consumption_municipality(territory_id);
CREATE INDEX idx_consumption_date ON consumption_municipality(date);
CREATE INDEX idx_consumption_category ON consumption_municipality(category);
CREATE INDEX idx_consumption_territory_date ON consumption_municipality(territory_id, date);
CREATE INDEX idx_consumption_date_category ON consumption_municipality(date, category);

-- Композитный индекс для аналитических запросов
CREATE INDEX idx_consumption_analytics ON consumption_municipality(category, date, territory_id)
    INCLUDE (consumption);

-- =============================================================================
-- 2. Таблица индекса доступности рынков
-- =============================================================================
CREATE TABLE IF NOT EXISTS market_access_municipality (
    market_access_id SERIAL PRIMARY KEY,
    territory_id VARCHAR(50) NOT NULL UNIQUE,
    -- Код территории (МО в постоянных границах)

    market_access NUMERIC(10, 4),
    -- Индекс доступности рынков в 2024 году (нормированный от 0 до 1000)
    -- Показывает относительный потенциальный объём внешнего рынка,
    -- доступный в муниципальном образовании

    year INTEGER DEFAULT 2024,
    -- Год расчёта индекса

    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

COMMENT ON TABLE market_access_municipality IS 'Индекс доступности рынков на уровне муниципальных образований (2024 год). Показывает относительный потенциальный объём внешнего рынка, доступный в МО. Более высокое значение говорит о более выгодном экономико-географическом положении. Источник: СберИндекс, лицензия CC BY-SA 4.0';

COMMENT ON COLUMN market_access_municipality.territory_id IS 'Код территории (муниципальное образование в постоянных границах). Отсутствуют 22 МО без постоянного автодорожного сообщения или паромного сообщения с материковой частью России';
COMMENT ON COLUMN market_access_municipality.market_access IS 'Индекс доступности рынков (нормированный от 0 до 1000). Рассчитывается как сумма населения всех МО, делённая на расстояние до них. Более высокое значение = более выгодное экономико-географическое положение и близость к крупным потребительским рынкам';
COMMENT ON COLUMN market_access_municipality.year IS 'Год, к которому относится расчёт индекса (2024)';

-- Индексы
CREATE INDEX idx_market_access_territory ON market_access_municipality(territory_id);
CREATE INDEX idx_market_access_value ON market_access_municipality(market_access DESC NULLS LAST);
CREATE INDEX idx_market_access_year ON market_access_municipality(year);

-- =============================================================================
-- 3. Справочник категорий потребления
-- =============================================================================
CREATE TABLE IF NOT EXISTS dict_consumption_categories (
    category_id SERIAL PRIMARY KEY,
    category_name VARCHAR(255) NOT NULL UNIQUE,
    description TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

COMMENT ON TABLE dict_consumption_categories IS 'Справочник категорий потребительских расходов СберИндекса';
COMMENT ON COLUMN dict_consumption_categories.category_name IS 'Название категории';
COMMENT ON COLUMN dict_consumption_categories.description IS 'Описание того, что входит в категорию';

-- Вставка категорий с описаниями
INSERT INTO dict_consumption_categories (category_name, description) VALUES
('Продовольствие', 'Покупка продуктов питания для домашнего приготовления и потребления: транзакции в супермаркетах, гипермаркетах, продуктовых магазинах у дома, на рынках и ярмарках, алкогольные напитки и табачные изделия'),
('Здоровье', 'Покупки лекарственных препаратов, медицинских изделий и сопутствующих товаров (по рецепту и без), услуги по диагностике, лечению и профилактике заболеваний медицинскими учреждениями'),
('Общественное питание', 'Услуги приготовления и продажи готовых блюд и напитков для немедленного потребления: рестораны, кафе, бары, фастфуд, столовые, доставка готовой еды, кейтеринг'),
('Транспорт', 'Услуги городского и пригородного общественного транспорта, такси, каршеринг, топливо, ремонт и техническое обслуживание автотранспорта, перевозка и доставка. Не включает авиа- и ж/д билеты'),
('Маркетплейсы', 'Транзакции на онлайн-платформах с множеством продавцов: одежда, электроника, бытовая техника, книги и др. Специализированные интернет-магазины относятся к соответствующим категориям'),
('Все категории', 'Включает все вышеперечисленные и иные категории потребительских расходов')
ON CONFLICT (category_name) DO NOTHING;

-- =============================================================================
-- Триггеры для автоматического обновления updated_at
-- =============================================================================
CREATE TRIGGER update_consumption_updated_at BEFORE UPDATE ON consumption_municipality
    FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();

CREATE TRIGGER update_market_access_updated_at BEFORE UPDATE ON market_access_municipality
    FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();

-- =============================================================================
-- Представления для аналитики
-- =============================================================================

-- Представление: средние расходы по категориям за последний месяц
CREATE OR REPLACE VIEW v_consumption_latest AS
SELECT
    c.territory_id,
    c.date,
    c.category,
    c.consumption,
    cat.description as category_description
FROM consumption_municipality c
JOIN dict_consumption_categories cat ON c.category = cat.category_name
WHERE c.date = (SELECT MAX(date) FROM consumption_municipality);

COMMENT ON VIEW v_consumption_latest IS 'Потребительские расходы за последний доступный месяц с описаниями категорий';

-- Представление: средние расходы по категориям за весь период
CREATE OR REPLACE VIEW v_consumption_avg_by_category AS
SELECT
    category,
    COUNT(DISTINCT territory_id) as territories_count,
    AVG(consumption) as avg_consumption,
    MIN(consumption) as min_consumption,
    MAX(consumption) as max_consumption,
    STDDEV(consumption) as stddev_consumption
FROM consumption_municipality
GROUP BY category;

COMMENT ON VIEW v_consumption_avg_by_category IS 'Статистика потребительских расходов по категориям (среднее, мин, макс, стд. отклонение)';

-- Представление: топ МО по индексу доступности рынков
CREATE OR REPLACE VIEW v_market_access_top AS
SELECT
    territory_id,
    market_access,
    RANK() OVER (ORDER BY market_access DESC) as rank
FROM market_access_municipality
WHERE market_access IS NOT NULL
ORDER BY market_access DESC;

COMMENT ON VIEW v_market_access_top IS 'Рейтинг муниципальных образований по индексу доступности рынков';

-- =============================================================================
-- Конец миграции 004
-- =============================================================================
