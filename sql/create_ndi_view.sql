-- ============================================================================
-- CREATE VIEW: sberindex.vw_ndi_calculation
-- ============================================================================
-- Цель: Рассчитать Northern Development Index (NDI) для всех 128 арктических НП
-- Формула: NDI = 0.35×POAD + 0.20×Market + 0.15×Consumption + 0.15×Access + 0.10×Climate + 0.05×Mobility
--
-- ФОРМАТЫ ВЫВОДА:
--   - ndi_score_100 (0-100 баллов) - РЕКОМЕНДУЕТСЯ для презентаций и дашбордов
--   - ndi_score (0-1)              - для технического анализа
--   - ndi_10 (0-10)                - для школьной шкалы оценок
--
-- Использование:
--   SELECT settlement_name, region_name, ndi_score_100, ndi_rank FROM sberindex.vw_ndi_calculation ORDER BY ndi_score_100 DESC LIMIT 10;
--   SELECT * FROM sberindex.vw_ndi_calculation WHERE region_name = 'Республика Карелия' ORDER BY ndi_score_100 DESC;
-- ============================================================================

DROP VIEW IF EXISTS sberindex.vw_ndi_calculation CASCADE;

CREATE VIEW sberindex.vw_ndi_calculation AS

WITH settlement_base AS (
    -- Базовая информация + маппинг НП → МО
    SELECT
        s.settlement_id,
        s.settlement_name,
        s.settlement_type,
        r.region_name,
        s.region_id,
        m_down.municipality_id::varchar as territory_id,
        LOWER(TRIM(m_down.municipality_name)) as municipality_name_normalized
    FROM sberindex.dict_settlements s
    JOIN sberindex.dict_regions r ON s.region_id = r.region_id
    LEFT JOIN sberindex.dict_municipalities m_down
        ON s.municipality_down_id = m_down.municipality_id
),

-- ============================================================================
-- КОМПОНЕНТ 1: POAD (35% веса) - Качество жизни
-- ============================================================================

poad_scores AS (
    SELECT settlement_id, poad_normalized
    FROM sberindex.poad_attractiveness_v1
),

-- ============================================================================
-- КОМПОНЕНТ 2: MARKET ACCESS (20% веса) - Доступность рынков
-- ============================================================================

market_access_raw AS (
    SELECT
        sb.settlement_id,
        sb.region_id,
        ma.market_access::numeric as market_raw
    FROM settlement_base sb
    LEFT JOIN sberindex.market_access_municipality ma
        ON ma.territory_id = sb.territory_id AND ma.year = 2024
),

market_access_with_regional_avg AS (
    -- Заполняем пропуски региональными средними
    SELECT
        settlement_id,
        COALESCE(
            market_raw,
            AVG(market_raw) OVER (PARTITION BY region_id)
        ) as market_filled
    FROM market_access_raw
),

market_access_normalized AS (
    SELECT
        settlement_id,
        -- Min-Max нормализация только по 128 арктическим НП
        (market_filled - MIN(market_filled) OVER ()) /
        NULLIF(MAX(market_filled) OVER () - MIN(market_filled) OVER (), 0) as market_normalized
    FROM market_access_with_regional_avg
),

-- ============================================================================
-- КОМПОНЕНТ 3: CONSUMPTION (15% веса) - Потребление
-- ============================================================================

consumption_raw AS (
    SELECT
        sb.settlement_id,
        sb.region_id,
        AVG(cm.consumption)::numeric as consumption_avg
    FROM settlement_base sb
    LEFT JOIN sberindex.consumption_municipality cm
        ON cm.territory_id = sb.territory_id
        AND cm.date >= '2024-07-01'
        AND cm.date <= '2024-12-31'
        AND cm.category = 'Все категории'
    GROUP BY sb.settlement_id, sb.region_id
),

consumption_with_living_wage AS (
    SELECT
        settlement_id,
        region_id,
        consumption_avg,
        -- Региональный прожиточный минимум
        CASE region_id
            WHEN 10 THEN 28000  -- Республика Карелия
            WHEN 11 THEN 28000  -- Республика Коми
            WHEN 29 THEN 27000  -- Архангельская область
            WHEN 47 THEN 20000  -- Ленинградская область
            WHEN 51 THEN 28000  -- Мурманская область
            WHEN 83 THEN 35000  -- Ненецкий АО
            WHEN 63 THEN 30000  -- Ямало-Ненецкий АО
            WHEN 71 THEN 32000  -- Тюменская область (ХМАО)
            WHEN 14 THEN 32000  -- Республика Саха (Якутия)
            WHEN 24 THEN 30000  -- Красноярский край
            WHEN 75 THEN 35000  -- Чукотский АО
            ELSE 27000
        END as living_wage
    FROM consumption_raw
),

consumption_with_regional_avg AS (
    -- Заполняем пропуски региональными средними
    SELECT
        settlement_id,
        COALESCE(
            consumption_avg,
            AVG(consumption_avg) OVER (PARTITION BY region_id)
        ) as consumption_filled,
        living_wage
    FROM consumption_with_living_wage
),

consumption_normalized AS (
    SELECT
        settlement_id,
        -- Welfare ratio: потребление / прожиточный минимум, clip at 2.0, normalize to 0-1
        LEAST(consumption_filled / living_wage, 2.0) / 2.0 as consumption_normalized
    FROM consumption_with_regional_avg
),

-- ============================================================================
-- КОМПОНЕНТ 4: ACCESSIBILITY (15% веса) - Доступность до хабов
-- ============================================================================

accessibility_normalized AS (
    SELECT settlement_id, accessibility_score as accessibility_normalized
    FROM sberindex.accessibility_scores
),

-- ============================================================================
-- КОМПОНЕНТ 5: CLIMATE (10% веса) - Климатическая нагрузка (HDD)
-- ============================================================================

climate_recent AS (
    -- Актуальный климат 2023-2024 (согласованность с Consumption и Market Access)
    -- Разбивка по сезонам для понимания экстремумов
    SELECT
        fcm.settlement_id,
        -- Среднегодовая (для совместимости)
        AVG(avg_temp_mean) as avg_temp_recent,

        -- Зимние месяцы (декабрь, январь, февраль) - ЭКСТРЕМУМ ХОЛОДА
        AVG(CASE WHEN month IN (12, 1, 2) THEN avg_temp_mean END) as avg_temp_winter,

        -- Летние месяцы (июнь, июль, август) - ЭКСТРЕМУМ ТЕПЛА
        AVG(CASE WHEN month IN (6, 7, 8) THEN avg_temp_mean END) as avg_temp_summer,

        -- Амплитуда (континентальность климата)
        AVG(CASE WHEN month IN (6, 7, 8) THEN avg_temp_mean END) -
        AVG(CASE WHEN month IN (12, 1, 2) THEN avg_temp_mean END) as temp_amplitude,

        -- HDD для расчёта индекса
        AVG(yearly.hdd_yearly) as avg_hdd_recent
    FROM sberindex.fact_climate_monthly fcm
    LEFT JOIN sberindex.agg_climate_yearly yearly
        ON fcm.settlement_id = yearly.settlement_id
        AND fcm.year = yearly.year
    WHERE fcm.year IN (2023, 2024)
    GROUP BY fcm.settlement_id
),

climate_normalized AS (
    SELECT
        cr.settlement_id,
        cr.avg_temp_recent as avg_temp_overall,
        cr.avg_temp_winter,
        cr.avg_temp_summer,
        cr.temp_amplitude,
        cr.avg_hdd_recent as avg_hdd_yearly,
        -- Инвертируем: холод (высокий HDD) = низкий балл
        1 - (
            (cr.avg_hdd_recent - MIN(cr.avg_hdd_recent) OVER ()) /
            NULLIF(MAX(cr.avg_hdd_recent) OVER () - MIN(cr.avg_hdd_recent) OVER (), 0)
        ) as climate_normalized
    FROM climate_recent cr
),

-- ============================================================================
-- КОМПОНЕНТ 6: MOBILITY (5% веса) - Мобильность населения
-- ============================================================================

mobility_raw AS (
    -- Реальные данные мобильности из mobility_index_municipality (2024)
    -- Индекс мобильности в км: среднее расстояние перемещения населения
    -- JOIN через название муниципалитета (нормализованное)
    SELECT
        sb.settlement_id,
        sb.region_id,
        mi.mobility_index_km as mobility_km
    FROM settlement_base sb
    LEFT JOIN sberindex.mobility_index_municipality mi
        ON LOWER(TRIM(mi.municipal_district_name)) = sb.municipality_name_normalized
        AND mi.year = 2024  -- Актуальный год (согласованность с consumption/market)
),

mobility_with_regional_avg AS (
    -- Fallback для НП без данных: средняя мобильность по региону
    SELECT
        mr.settlement_id,
        COALESCE(
            mr.mobility_km,
            AVG(mr.mobility_km) OVER (PARTITION BY mr.region_id)
        ) as mobility_filled
    FROM mobility_raw mr
),

mobility_scores AS (
    -- Min-Max нормализация (больше км = лучше мобильность = выше балл)
    SELECT
        settlement_id,
        mobility_filled,
        COALESCE(
            (mobility_filled - MIN(mobility_filled) OVER ()) /
            NULLIF(MAX(mobility_filled) OVER () - MIN(mobility_filled) OVER (), 0),
            0.5  -- Fallback если все NULL
        ) as mobility_normalized
    FROM mobility_with_regional_avg
),

-- ============================================================================
-- ФИНАЛЬНЫЙ РАСЧЁТ NDI
-- ============================================================================

ndi_components AS (
    SELECT
        sb.settlement_id,
        sb.settlement_name,
        sb.settlement_type,
        sb.region_name,

        -- Компоненты (нормализованные 0-1)
        COALESCE(p.poad_normalized, 0.5) as poad_score,
        COALESCE(m.market_normalized, 0.5) as market_score,
        COALESCE(c.consumption_normalized, 0.5) as consumption_score,
        COALESCE(a.accessibility_normalized, 0.5) as accessibility_score,
        COALESCE(cl.climate_normalized, 0.5) as climate_score,
        COALESCE(mo.mobility_normalized, 0.5) as mobility_score,

        -- Дополнительные данные для анализа
        cl.avg_temp_overall,
        cl.avg_temp_winter,
        cl.avg_temp_summer,
        cl.temp_amplitude,
        cl.avg_hdd_yearly,

        -- Мобильность в км (реальное значение)
        mo.mobility_filled as mobility_km

    FROM settlement_base sb
    LEFT JOIN poad_scores p ON sb.settlement_id = p.settlement_id
    LEFT JOIN market_access_normalized m ON sb.settlement_id = m.settlement_id
    LEFT JOIN consumption_normalized c ON sb.settlement_id = c.settlement_id
    LEFT JOIN accessibility_normalized a ON sb.settlement_id = a.settlement_id
    LEFT JOIN climate_normalized cl ON sb.settlement_id = cl.settlement_id
    LEFT JOIN mobility_scores mo ON sb.settlement_id = mo.settlement_id
)

SELECT
    nc.settlement_id,
    nc.settlement_name,
    nc.settlement_type,
    nc.region_name,

    -- Флаги арктических территорий
    COALESCE(msa.is_arctic, false) as is_arctic,
    COALESCE(msa.is_remote, false) as is_remote,
    COALESCE(msa.is_special, false) as is_special,
    COALESCE(msa.is_suburb, false) as is_suburb,

    -- Компоненты (0-1) - для технического анализа
    ROUND(nc.poad_score::numeric, 4) as poad_score,
    ROUND(nc.market_score::numeric, 4) as market_score,
    ROUND(nc.consumption_score::numeric, 4) as consumption_score,
    ROUND(nc.accessibility_score::numeric, 4) as accessibility_score,
    ROUND(nc.climate_score::numeric, 4) as climate_score,
    ROUND(nc.mobility_score::numeric, 4) as mobility_score,

    -- Компоненты в баллах (0-100) - для презентаций и дашбордов
    ROUND(nc.poad_score::numeric * 100, 2) as poad_score_100,
    ROUND(nc.market_score::numeric * 100, 2) as market_score_100,
    ROUND(nc.consumption_score::numeric * 100, 2) as consumption_score_100,
    ROUND(nc.accessibility_score::numeric * 100, 2) as accessibility_score_100,
    ROUND(nc.climate_score::numeric * 100, 2) as climate_score_100,
    ROUND(nc.mobility_score::numeric * 100, 2) as mobility_score_100,

    -- Финальный NDI (0-1) - для совместимости
    ROUND(
        (0.35 * nc.poad_score +
         0.20 * nc.market_score +
         0.15 * nc.consumption_score +
         0.15 * nc.accessibility_score +
         0.10 * nc.climate_score +
         0.05 * nc.mobility_score)::numeric
    , 4) as ndi_score,

    -- Финальный NDI в баллах (0-100) - РЕКОМЕНДУЕТСЯ для использования
    ROUND(
        (0.35 * nc.poad_score +
         0.20 * nc.market_score +
         0.15 * nc.consumption_score +
         0.15 * nc.accessibility_score +
         0.10 * nc.climate_score +
         0.05 * nc.mobility_score)::numeric * 100
    , 2) as ndi_score_100,

    -- Финальный NDI (0-10) - для дашборда
    ROUND(
        (0.35 * nc.poad_score +
         0.20 * nc.market_score +
         0.15 * nc.consumption_score +
         0.15 * nc.accessibility_score +
         0.10 * nc.climate_score +
         0.05 * nc.mobility_score)::numeric * 10
    , 2) as ndi_10,

    -- Цветовая индикация для дашборда
    CASE
        WHEN (0.35 * nc.poad_score + 0.20 * nc.market_score + 0.15 * nc.consumption_score +
              0.15 * nc.accessibility_score + 0.10 * nc.climate_score + 0.05 * nc.mobility_score) * 10 < 3.0
            THEN '#d32f2f'  -- Критический (тёмно-красный)
        WHEN (0.35 * nc.poad_score + 0.20 * nc.market_score + 0.15 * nc.consumption_score +
              0.15 * nc.accessibility_score + 0.10 * nc.climate_score + 0.05 * nc.mobility_score) * 10 < 4.5
            THEN '#f57c00'  -- Низкий (оранжевый)
        WHEN (0.35 * nc.poad_score + 0.20 * nc.market_score + 0.15 * nc.consumption_score +
              0.15 * nc.accessibility_score + 0.10 * nc.climate_score + 0.05 * nc.mobility_score) * 10 < 6.5
            THEN '#fbc02d'  -- Средний (жёлтый)
        ELSE '#388e3c'  -- Высокий (зелёный)
    END as color_ndi,

    -- Ранг (вычисляется динамически при запросе)
    RANK() OVER (ORDER BY
        (0.35 * nc.poad_score +
         0.20 * nc.market_score +
         0.15 * nc.consumption_score +
         0.15 * nc.accessibility_score +
         0.10 * nc.climate_score +
         0.05 * nc.mobility_score) DESC
    ) as ndi_rank,

    -- Дополнительные данные: климат (среднегодовая)
    ROUND(nc.avg_temp_overall::numeric, 1) as avg_temp_celsius,

    -- Сезонные экстремумы (ИНТУИТИВНО ПОНЯТНЫ!)
    ROUND(nc.avg_temp_winter::numeric, 1) as avg_temp_winter_celsius,
    ROUND(nc.avg_temp_summer::numeric, 1) as avg_temp_summer_celsius,
    ROUND(nc.temp_amplitude::numeric, 1) as temp_amplitude_celsius,

    -- Технические метрики
    nc.avg_hdd_yearly,

    -- Мобильность (реальное значение в км)
    ROUND(nc.mobility_km::numeric, 2) as mobility_index_km,

    -- Категория климата (человекочитаемая)
    CASE
        WHEN nc.avg_hdd_yearly < 5000 THEN 'Мягкий'
        WHEN nc.avg_hdd_yearly < 6000 THEN 'Холодный'
        WHEN nc.avg_hdd_yearly < 7500 THEN 'Очень холодный'
        WHEN nc.avg_hdd_yearly < 9000 THEN 'Арктический'
        ELSE 'Экстремальный'
    END as climate_category,

    -- Разница с Москвой (в % дороже на отопление)
    ROUND((nc.avg_hdd_yearly - 4500) / 4500.0 * 100, 0) as heating_cost_vs_moscow_percent

FROM ndi_components nc
LEFT JOIN sberindex.meta_settlement_attributes msa
    ON nc.settlement_id = msa.settlement_id
ORDER BY ndi_score DESC;

-- ============================================================================
-- КОММЕНТАРИЙ К VIEW
-- ============================================================================

COMMENT ON VIEW sberindex.vw_ndi_calculation IS
'Northern Development Index (NDI) для 128 арктических НП.
Формула: NDI = 0.35×POAD + 0.20×Market + 0.15×Consumption + 0.15×Access + 0.10×Climate + 0.05×Mobility
Источники: POAD (attractiveness_v1.csv), Market (market_access_municipality - 2024), Consumption (consumption_municipality - июль-дек 2024),
Accessibility (accessibility_scores.csv), Climate (agg_climate_yearly - АКТУАЛЬНЫЙ климат 2023-2024), Mobility (mobility_index_municipality - 2024)

ВАЖНО: Климат синхронизирован с периодом других компонентов (2023-2024) для согласованности данных.
 
Климатические данные:
- avg_temp_celsius: среднегодовая температура (°C) за 2023-2024 гг.
- avg_temp_winter_celsius: средняя зимняя температура дек-янв-фев (°C) - ЭКСТРЕМУМ ХОЛОДА
- avg_temp_summer_celsius: средняя летняя температура июн-июл-авг (°C) - ЭКСТРЕМУМ ТЕПЛА
- temp_amplitude_celsius: амплитуда температур (лето-зима) - показатель континентальности
- avg_hdd_yearly: Heating Degree Days (градусо-дни/год) - мера холода за 2023-2024 гг.
- climate_category: категория климата (Мягкий / Холодный / Очень холодный / Арктический / Экстремальный)
- heating_cost_vs_moscow_percent: разница в затратах на отопление относительно Москвы (%)
 
 