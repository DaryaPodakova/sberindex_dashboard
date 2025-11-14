# Быстрый старт: СберИндекс POAD ETL

## 🚀 Установка и запуск за 3 шага

### Шаг 1: Применить миграции БД

```bash
cd /workspaces/pet_project/sberbank_hackaton_11_2025/arctic
python3 apply_migrations.py
```

**Результат:**
- Создана схема `sberindex`
- Созданы 12 таблиц (8 справочных, 3 метаданных, 1 фактовая)
- Созданы индексы и триггеры

---

### Шаг 2: Загрузить данные из Excel

```bash
python3 etl_load_sberindex.py --excel-path ../Data_SberIndex_POAD.xlsx --env-path ../../.env
```

**Результат:**
- Загружено 10 регионов
- Загружено 192 муниципалитета
- Загружено 128 населённых пунктов
- Загружено 93 показателя
- Загружено 8,159 фактических записей
- **Время выполнения: ~7 секунд**

---

### Шаг 3: Проверить данные

```bash
# Показать доступные запросы
python3 query_helper.py --list

# Статистика по таблицам
python3 query_helper.py --query stats

# Арктические населённые пункты
python3 query_helper.py --query arctic

# Топ-10 по населению
python3 query_helper.py --query population

# Статистика по регионам
python3 query_helper.py --query regions

# Покрытие данными
python3 query_helper.py --query coverage
```

---

## 📊 Примеры SQL запросов

### 1. Все показатели для конкретного НП

```sql
SELECT
    i.indicator_name AS "Показатель",
    f.value_numeric AS "Значение"
FROM sberindex.fact_settlement_indicators f
JOIN sberindex.dict_settlements s ON f.settlement_id = s.settlement_id
JOIN sberindex.dict_indicators i ON f.indicator_id = i.indicator_id
WHERE s.settlement_name LIKE '%Якутск%'
ORDER BY i.indicator_name;
```

### 2. Арктические НП с координатами и населением

```sql
SELECT
    s.settlement_name AS "Населённый пункт",
    r.region_name AS "Регион",
    c.latitude AS "Широта",
    c.longitude AS "Долгота",
    p.population_total AS "Население"
FROM sberindex.dict_settlements s
JOIN sberindex.dict_regions r ON s.region_id = r.region_id
JOIN sberindex.meta_settlement_attributes a ON s.settlement_id = a.settlement_id
JOIN sberindex.meta_settlement_coordinates c ON s.settlement_id = c.settlement_id
LEFT JOIN sberindex.meta_settlement_population p ON s.settlement_id = p.settlement_id
WHERE a.is_arctic = TRUE
ORDER BY p.population_total DESC NULLS LAST;
```

### 3. Средние значения показателя по типам НП

```sql
SELECT
    s.settlement_type AS "Тип НП",
    COUNT(DISTINCT s.settlement_id) AS "Количество НП",
    AVG(f.value_numeric) AS "Среднее значение"
FROM sberindex.fact_settlement_indicators f
JOIN sberindex.dict_settlements s ON f.settlement_id = s.settlement_id
JOIN sberindex.dict_indicators i ON f.indicator_id = i.indicator_id
WHERE i.acronym = 'wage_average'  -- средняя зарплата
GROUP BY s.settlement_type
ORDER BY AVG(f.value_numeric) DESC;
```

### 4. Сравнение регионов по показателям

```sql
SELECT
    r.region_name AS "Регион",
    COUNT(DISTINCT s.settlement_id) AS "НП всего",
    SUM(CASE WHEN a.is_arctic THEN 1 ELSE 0 END) AS "Арктических",
    AVG(p.population_total) AS "Средн. население"
FROM sberindex.dict_regions r
JOIN sberindex.dict_settlements s ON r.region_id = s.region_id
LEFT JOIN sberindex.meta_settlement_attributes a ON s.settlement_id = a.settlement_id
LEFT JOIN sberindex.meta_settlement_population p ON s.settlement_id = p.settlement_id
GROUP BY r.region_name
ORDER BY COUNT(DISTINCT s.settlement_id) DESC;
```

---
    - Атрибуты (arctic, remote, special, suburb)
meta_settlement_coordinates  - Географические координаты
meta_settlement_population   - Демография (население, м/ж)
```

### Факты (fact_*)
```
fact_settlement_indicators   - Нормализованная таблица показателей
                               (settlement_id + indicator_id + value)
```

---

## 🔧 Устранение неполадок

### Ошибка подключения к БД

```bash
# Проверьте .env файл
cat ../.env | grep DB_

# Проверьте доступность PostgreSQL
psql -h host.docker.internal -p 5432 -U bot_etl_user2 -d platform
```

### Миграции уже применены

Миграции используют `IF NOT EXISTS`, поэтому безопасны для повторного запуска.

### Очистка данных для повторной загрузки

```sql
-- Удалить только данные, сохранив структуру
TRUNCATE sberindex.fact_settlement_indicators CASCADE;
TRUNCATE sberindex.dict_settlements CASCADE;
TRUNCATE sberindex.dict_municipalities CASCADE;
TRUNCATE sberindex.dict_regions CASCADE;

-- ИЛИ удалить всю схему
DROP SCHEMA sberindex CASCADE;
```

---

## 📈 Что дальше?

1. **Анализ данных** - используйте SQL запросы или Python
2. **Визуализация** - подключите Power BI, Tableau, Superset
3. **API** - создайте REST API для доступа к данным
4. **dbt трансформации** - добавьте аналитические слои
5. **ML модели** - используйте данные для обучения моделей

---

## 📚 Документация

- [README_ETL.md](README_ETL.md) - Полная документация
- [SUMMARY.md](SUMMARY.md) - Краткая сводка результатов
- `sql/` - SQL миграции
- `etl_load_sberindex.py` - ETL код (670 строк)

---

## 💡 Полезные команды

```bash
# Размер таблиц
SELECT
    tablename,
    pg_size_pretty(pg_total_relation_size('sberindex.'||tablename)) AS size
FROM pg_tables
WHERE schemaname = 'sberindex'
ORDER BY pg_total_relation_size('sberindex.'||tablename) DESC;

# Проверка индексов
SELECT
    tablename,
    indexname,
    indexdef
FROM pg_indexes
WHERE schemaname = 'sberindex';

# Количество записей во всех таблицах
SELECT
    'regions' as table_name, COUNT(*) FROM sberindex.dict_regions
UNION ALL SELECT 'municipalities', COUNT(*) FROM sberindex.dict_municipalities
UNION ALL SELECT 'settlements', COUNT(*) FROM sberindex.dict_settlements
UNION ALL SELECT 'indicators', COUNT(*) FROM sberindex.dict_indicators
UNION ALL SELECT 'facts', COUNT(*) FROM sberindex.fact_settlement_indicators;
```

---

**Готово!** 🎉

Все данные нормализованы и загружены в PostgreSQL в схему `sberindex`.
