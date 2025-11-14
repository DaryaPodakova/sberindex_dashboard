# SQL Scripts for Arctic NDI

## create_ndi_view.sql

**Финальный скрипт расчёта Northern Development Index (NDI)**

### Описание

Создаёт PostgreSQL VIEW `sberindex.vw_ndi_calculation`, который вычисляет композитный индекс устойчивости арктических населённых пунктов.

### Формула NDI

```
NDI = 0.35×POAD + 0.20×Market + 0.15×Consumption +
      0.15×Access + 0.10×Climate + 0.05×Mobility
```

### Компоненты (вес):

1. **POAD (35%)** — Качество жизни (медицина, образование, культура)
2. **Market Access (20%)** — Доступность рынков (экономическая интеграция)
3. **Consumption (15%)** — Покупательная способность (потребление vs прожиточный минимум)
4. **Accessibility (15%)** — Транспортная доступность (близость к хабам)
5. **Climate (10%)** — Климат (затраты на отопление, HDD)
6. **Mobility (5%)** — Мобильность населения (экономическая активность)

### Технические детали

- **Размер:** 22 КБ, 390 строк SQL
- **Схема:** `sberindex`
- **VIEW:** `vw_ndi_calculation`
- **Выход:** 128 арктических НП с оценками 0-100

### Ключевые операции:

1. **Нормализация компонентов** (Min-Max scaling 0-1)
2. **Заполнение пропусков** (региональные средние, fallback 0.5)
3. **Weighted sum** с весами компонентов
4. **Масштабирование** в диапазон 0-100
5. **Ранжирование** по итоговому NDI

### Использование

```bash
# Создать VIEW в PostgreSQL
psql -U bot_etl_user2 -d platform -f create_ndi_view.sql

# Запросить данные
psql -U bot_etl_user2 -d platform -c "
  SELECT settlement_name, region_name, ndi_score_100, ndi_rank
  FROM sberindex.vw_ndi_calculation
  ORDER BY ndi_score_100 DESC
  LIMIT 10;
"
```

### Зависимости (таблицы БД):

- `sberindex.dict_settlements` — справочник 128 НП
- `sberindex.poad_attractiveness_v1` — индекс POAD
- `sberindex.market_access_municipality` — доступность рынков
- `sberindex.consumption_municipality` — потребление
- `sberindex.accessibility_scores` — транспортная доступность
- `sberindex.agg_climate_yearly` — климат (HDD)
- `sberindex.mobility_index_municipality` — мобильность
- `sberindex.dict_regions` — справочник регионов

### Выходные колонки VIEW:

**Идентификаторы:**
- `settlement_id`, `settlement_name`, `region_name`

**Компоненты (0-1 и 0-100):**
- `poad_score`, `poad_score_100`
- `market_score`, `market_score_100`
- `consumption_score`, `consumption_score_100`
- `accessibility_score`, `accessibility_score_100`
- `climate_score`, `climate_score_100`
- `mobility_score`, `mobility_score_100`

**Итоговый NDI:**
- `ndi_score` (0-1)
- `ndi_score_100` (0-100)
- `ndi_10` (0-10)
- `ndi_rank` (1-128)
- `color_ndi` (цветовая категория)

**Климатические метрики:**
- `avg_temp_celsius`, `avg_temp_winter_celsius`, `avg_temp_summer_celsius`
- `temp_amplitude_celsius`, `avg_hdd_yearly`
- `climate_category`, `heating_cost_vs_moscow_percent`

**Мобильность:**
- `mobility_index_km`

### Примеры запросов

```sql
-- Топ-10 по NDI
SELECT settlement_name, ndi_score_100, ndi_rank
FROM sberindex.vw_ndi_calculation
ORDER BY ndi_score_100 DESC LIMIT 10;

-- НП в зоне риска (NDI < 40)
SELECT settlement_name, ndi_score_100,
       poad_score_100, climate_score_100
FROM sberindex.vw_ndi_calculation
WHERE ndi_score_100 < 40
ORDER BY ndi_score_100 ASC;

-- Статистика по регионам
SELECT region_name,
       COUNT(*) as settlements,
       ROUND(AVG(ndi_score_100), 2) as avg_ndi
FROM sberindex.vw_ndi_calculation
GROUP BY region_name
ORDER BY avg_ndi DESC;
```

### Источник

Скопировано из: `sberbank_hackaton_11_2025/scripts/create_ndi_view.sql`

### Документация

См. также:
- [../docs/methodology_ndi.md](../docs/methodology_ndi.md) — методология расчёта
- [../docs/REPRODUCE_NDI.md](../docs/REPRODUCE_NDI.md) — воспроизводимость

---

Последнее обновление: 2025-11-14
