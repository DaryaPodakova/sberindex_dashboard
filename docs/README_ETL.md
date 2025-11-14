# ETL для данных СберИндекса POAD

Система нормализации и загрузки данных СберИндекса POAD из Excel в PostgreSQL.

## Структура схемы БД

Все таблицы создаются в отдельной схеме `sberindex`.

### Справочные таблицы (dictionaries)

1. **dict_regions** - Регионы РФ
2. **dict_municipalities** - Муниципалитеты (верхний/нижний уровень)
3. **dict_settlements** - Населённые пункты
4. **dict_data_blocks** - Блоки данных (ИНСТРУМЕНТАЛЬНЫЕ, ESG и др.)
5. **dict_rating_domains** - Домены рейтинга (ДОХОДЫ, ЖИЛЬЕ и др.)
6. **dict_data_sources** - Источники данных
7. **dict_indicators** - Показатели из Кодбука
8. **dict_esg_blocks** - ESG блоки и цели устойчивого развития

### Метаданные

1. **meta_settlement_attributes** - Характеристики (arctic, remote, special, suburb)
2. **meta_settlement_coordinates** - Географические координаты
3. **meta_settlement_population** - Демографические данные

### Факт-таблицы

1. **fact_settlement_indicators** - Нормализованная таблица показателей
   - Вместо 94 колонок → строки (settlement_id, indicator_id, value)
   - Поддержка числовых, текстовых и булевых значений
   - Индексы для быстрых аналитических запросов

2. **link_indicator_esg** - Связь показателей с ESG блоками (many-to-many)

3. **audit_settlement_indicators** - Журнал изменений данных

## Установка и запуск

### 1. Требования

```bash
# Python пакеты
pip install pandas openpyxl psycopg2-binary python-dotenv

# Или через requirements
pip install -r requirements.txt
```

### 2. Настройка .env

Создайте или дополните `.env` файл:

```env
DB_HOST=localhost
DB_PORT=5432
DB_NAME=platform
DB_USER=bot_etl_user2
DB_PASSWORD=your_password_here
```

### 3. Применение миграций

```bash
cd sberbank_hackaton_11_2025

# Применить все миграции (создание схемы и таблиц)
python apply_migrations.py
```

Это выполнит последовательно:
- `001_create_schema.sql` - создание схемы sberindex
- `002_create_dictionaries.sql` - справочные таблицы
- `003_create_fact_tables.sql` - факт-таблицы и метаданные

### 4. Загрузка данных

```bash
# Загрузить данные из Excel
python etl_load_sberindex.py --excel-path Data_SberIndex_POAD.xlsx
```

## Процесс ETL

### Этапы загрузки

1. **Справочники**
   - Блоки данных
   - Домены рейтинга
   - Источники данных
   - ESG блоки
   - Показатели (из Кодбука)

2. **География**
   - Регионы
   - Муниципалитеты (верхний и нижний уровень)
   - Населённые пункты

3. **Метаданные**
   - Атрибуты населённых пунктов
   - Координаты
   - Демография

4. **Факты**
   - Нормализация 94 колонок → строки в fact_settlement_indicators
   - Пакетная загрузка (batch insert)

### Нормализация данных

**До** (денормализованная структура):
```
settlement_name | wage_average | pop_total | kindergarden_salary | ...
Якутск          | 85000        | 350000    | 45000              | ... (94 колонки)
```

**После** (нормализованная структура):
```sql
-- fact_settlement_indicators
settlement_id | indicator_id | value_numeric
1            | 18           | 85000.00      -- wage_average
1            | 13           | 350000.00     -- pop_total
1            | 20           | 45000.00      -- kindergarden_salary
```

## Примеры запросов

### 1. Получить все показатели для населённого пункта

```sql
SELECT
    s.settlement_name,
    i.indicator_name,
    i.acronym,
    f.value_numeric,
    f.value_text,
    f.value_boolean
FROM sberindex.fact_settlement_indicators f
JOIN sberindex.dict_settlements s ON f.settlement_id = s.settlement_id
JOIN sberindex.dict_indicators i ON f.indicator_id = i.indicator_id
WHERE s.settlement_name = 'Город Якутск';
```

### 2. Статистика по арктическим населённым пунктам

```sql
SELECT
    r.region_name,
    COUNT(DISTINCT s.settlement_id) as arctic_settlements,
    AVG(p.population_total) as avg_population
FROM sberindex.dict_settlements s
JOIN sberindex.dict_regions r ON s.region_id = r.region_id
JOIN sberindex.meta_settlement_attributes a ON s.settlement_id = a.settlement_id
LEFT JOIN sberindex.meta_settlement_population p ON s.settlement_id = p.settlement_id
WHERE a.is_arctic = TRUE
GROUP BY r.region_name
ORDER BY arctic_settlements DESC;
```

### 3. Сравнение средней зарплаты по типам НП

```sql
SELECT
    s.settlement_type,
    COUNT(*) as count,
    AVG(f.value_numeric) as avg_wage
FROM sberindex.fact_settlement_indicators f
JOIN sberindex.dict_settlements s ON f.settlement_id = s.settlement_id
JOIN sberindex.dict_indicators i ON f.indicator_id = i.indicator_id
WHERE i.acronym = 'wage_average'
GROUP BY s.settlement_type
ORDER BY avg_wage DESC;
```

### 4. Показатели по ESG блокам

```sql
SELECT
    e.block_name,
    i.indicator_name,
    COUNT(DISTINCT f.settlement_id) as settlements_count,
    AVG(f.value_numeric) as avg_value
FROM sberindex.link_indicator_esg l
JOIN sberindex.dict_esg_blocks e ON l.esg_block_id = e.esg_block_id
JOIN sberindex.dict_indicators i ON l.indicator_id = i.indicator_id
JOIN sberindex.fact_settlement_indicators f ON i.indicator_id = f.indicator_id
GROUP BY e.block_name, i.indicator_name;
```

## Архитектурные преимущества

### ✅ Нормализация (3NF)
- Нет дублирования данных
- Легко добавлять новые показатели
- Консистентность справочников

### ✅ Масштабируемость
- Пакетная загрузка данных
- Индексы для аналитических запросов
- Connection pooling

### ✅ Аудит
- Триггеры обновления updated_at
- Опциональный audit_log

### ✅ Гибкость
- Поддержка разных типов значений (numeric, text, boolean)
- Возможность версионирования (year поле)
- Связь показателей с ESG блоками

## Структура файлов

```
sberbank_hackaton_11_2025/
├── sql/
│   ├── 001_create_schema.sql          # Создание схемы
│   ├── 002_create_dictionaries.sql    # Справочники
│   └── 003_create_fact_tables.sql     # Факт-таблицы
├── Data_SberIndex_POAD.xlsx           # Исходные данные
├── apply_migrations.py                # Скрипт применения миграций
├── etl_load_sberindex.py             # Основной ETL скрипт
└── README_ETL.md                      # Документация (этот файл)
```

## Troubleshooting

### Ошибка подключения к БД

```bash
# Проверьте .env файл
cat .env | grep DB_

# Проверьте доступность БД
psql -h localhost -p 5432 -U bot_etl_user2 -d platform
```

### Миграции уже применены

Миграции используют `IF NOT EXISTS` и `ON CONFLICT`, поэтому безопасны для повторного запуска.

### Очистка данных

```sql
-- Удалить все данные из схемы (осторожно!)
DROP SCHEMA sberindex CASCADE;

-- Или только факт-таблицу
TRUNCATE sberindex.fact_settlement_indicators CASCADE;
```

## Мониторинг

### Проверка загруженных данных

```sql
-- Количество записей в таблицах
SELECT
    'regions' as table_name,
    COUNT(*) FROM sberindex.dict_regions
UNION ALL SELECT 'municipalities', COUNT(*) FROM sberindex.dict_municipalities
UNION ALL SELECT 'settlements', COUNT(*) FROM sberindex.dict_settlements
UNION ALL SELECT 'indicators', COUNT(*) FROM sberindex.dict_indicators
UNION ALL SELECT 'facts', COUNT(*) FROM sberindex.fact_settlement_indicators;
```

### Размер таблиц

```sql
SELECT
    schemaname,
    tablename,
    pg_size_pretty(pg_total_relation_size(schemaname||'.'||tablename)) AS size
FROM pg_tables
WHERE schemaname = 'sberindex'
ORDER BY pg_total_relation_size(schemaname||'.'||tablename) DESC;
```

## Дальнейшее развитие

- [ ] Добавить валидацию данных
- [ ] Создать представления (views) для частых запросов
- [ ] Добавить поддержку исторических данных (версионирование)
- [ ] Интеграция с dbt для аналитических трансформаций
- [ ] API endpoints для доступа к данным
