# Отчет о загрузке данных из Parquet файлов

## ✅ Что загружено

### 1. Потребительские расходы (consumption_municipality)
- **303,126 записей** безналичных потребительских расходов
- **Период**: январь 2023 - декабрь 2024 (24 месяца)
- **Территорий**: 2,190 муниципальных образований
- **Категории**: 6 категорий трат

#### Статистика по категориям:

| Категория | Записей | Средний расход (руб/мес) |
|-----------|---------|--------------------------|
| Все категории | 50,521 | 28,566.24 |
| Продовольствие | 50,521 | 11,987.10 |
| Маркетплейсы | 50,521 | 3,873.02 |
| Транспорт | 50,521 | 1,700.07 |
| Здоровье | 50,521 | 1,393.48 |
| Общественное питание | 50,521 | 1,139.20 |

#### Описание категорий:

**Продовольствие** - Покупка продуктов питания для домашнего приготовления: супермаркеты, гипермаркеты, продуктовые магазины, рынки, алкоголь, табак.

**Здоровье** - Лекарственные препараты, медицинские изделия, услуги диагностики, лечения и профилактики заболеваний.

**Общественное питание** - Рестораны, кафе, бары, фастфуд, столовые, доставка готовой еды, кейтеринг.

**Транспорт** - Городской/пригородный транспорт, такси, каршеринг, топливо, ремонт авто, перевозка (без авиа и ж/д).

**Маркетплейсы** - Онлайн-платформы с множеством продавцов: одежда, электроника, бытовая техника, книги и др.

**Все категории** - Включает все вышеперечисленные и иные категории потребительских расходов.

---

### 2. Индекс доступности рынков (market_access_municipality)
- **2,571 территорий** с индексом доступности
- **Год**: 2024
- **Диапазон индекса**: 110.30 - 1,000.00
- **Отсутствуют**: 22 МО без постоянного автодорожного или паромного сообщения

#### Что показывает индекс:
Относительный потенциальный объём внешнего рынка, доступный в муниципальном образовании. Более высокое значение = более выгодное экономико-географическое положение и близость к крупным потребительским рынкам.

**Формула расчёта:**
MA = Σ (Население_МО_d / Расстояние_до_МО_d)
Значения нормированы от 0 до 1,000.

---

## 📊 Структура таблиц в БД

### consumption_municipality
```sql
CREATE TABLE sberindex.consumption_municipality (
    consumption_id BIGSERIAL PRIMARY KEY,
    territory_id VARCHAR(50) NOT NULL,        -- Код территории (МО)
    date DATE NOT NULL,                       -- Год и месяц
    category VARCHAR(255) NOT NULL,           -- Категория расходов
    consumption NUMERIC(15, 2),               -- Средние расходы (руб)
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(territory_id, date, category)
);
```

**Индексы:**
- `idx_consumption_territory` - быстрый поиск по территории
- `idx_consumption_date` - фильтрация по дате
- `idx_consumption_category` - группировка по категориям
- `idx_consumption_analytics` - композитный для аналитики

### market_access_municipality
```sql
CREATE TABLE sberindex.market_access_municipality (
    market_access_id SERIAL PRIMARY KEY,
    territory_id VARCHAR(50) NOT NULL UNIQUE, -- Код территории
    market_access NUMERIC(10, 4),             -- Индекс 0-1000
    year INTEGER DEFAULT 2024,                -- Год расчёта
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
```

**Индексы:**
- `idx_market_access_territory` - поиск по территории
- `idx_market_access_value` - сортировка по значению индекса
- `idx_market_access_year` - фильтр по году

---

## 🔗 Возможности соединения с другими таблицами

### Связь через territory_id

```sql
-- Присоединение к данным POAD по населённым пунктам (через ОКТМО)
SELECT
    c.territory_id,
    c.category,
    c.consumption,
    s.settlement_name,
    r.region_name
FROM sberindex.consumption_municipality c
LEFT JOIN sberindex.dict_settlements s
    ON c.territory_id = s.oktmo_code  -- требует доработки маппинга
LEFT JOIN sberindex.dict_regions r
    ON s.region_id = r.region_id;
```

### Связь market_access + consumption

```sql
-- Анализ потребления в зависимости от доступности рынков
SELECT
    m.market_access,
    CASE
        WHEN m.market_access >= 800 THEN 'Высокая'
        WHEN m.market_access >= 500 THEN 'Средняя'
        ELSE 'Низкая'
    END as accessibility_level,
    c.category,
    AVG(c.consumption) as avg_consumption
FROM sberindex.market_access_municipality m
JOIN sberindex.consumption_municipality c
    ON m.territory_id = c.territory_id
WHERE c.date >= '2024-01-01'
GROUP BY m.market_access, 2, c.category;
```

---

## 📈 Примеры аналитических запросов

### 1. Динамика потребления по месяцам

```sql
SELECT
    DATE_TRUNC('month', date) as month,
    category,
    AVG(consumption) as avg_consumption,
    COUNT(DISTINCT territory_id) as territories
FROM sberindex.consumption_municipality
GROUP BY 1, 2
ORDER BY 1, 2;
```

### 2. Топ-20 территорий по расходам в категории

```sql
SELECT
    territory_id,
    category,
    AVG(consumption) as avg_monthly_consumption
FROM sberindex.consumption_municipality
WHERE category = 'Продовольствие'
  AND date >= '2024-01-01'
GROUP BY territory_id, category
ORDER BY avg_monthly_consumption DESC
LIMIT 20;
```

### 3. Корреляция доступности рынков и расходов

```sql
WITH consumption_avg AS (
    SELECT
        territory_id,
        AVG(consumption) FILTER (WHERE category = 'Все категории') as avg_total_consumption
    FROM sberindex.consumption_municipality
    WHERE date >= '2024-01-01'
    GROUP BY territory_id
)
SELECT
    CASE
        WHEN m.market_access >= 800 THEN '>= 800'
        WHEN m.market_access >= 600 THEN '600-800'
        WHEN m.market_access >= 400 THEN '400-600'
        WHEN m.market_access >= 200 THEN '200-400'
        ELSE '< 200'
    END as market_access_range,
    COUNT(*) as territories_count,
    AVG(c.avg_total_consumption) as avg_consumption,
    MIN(c.avg_total_consumption) as min_consumption,
    MAX(c.avg_total_consumption) as max_consumption
FROM sberindex.market_access_municipality m
JOIN consumption_avg c ON m.territory_id = c.territory_id
GROUP BY 1
ORDER BY MIN(m.market_access);
```

### 4. Сезонность по категориям

```sql
SELECT
    EXTRACT(MONTH FROM date) as month,
    category,
    AVG(consumption) as avg_consumption
FROM sberindex.consumption_municipality
GROUP BY 1, 2
ORDER BY 1, 2;
```

### 5. Рост/падение потребления год к году

```sql
SELECT
    territory_id,
    category,
    AVG(CASE WHEN date >= '2024-01-01' THEN consumption END) as consumption_2024,
    AVG(CASE WHEN date < '2024-01-01' THEN consumption END) as consumption_2023,
    (AVG(CASE WHEN date >= '2024-01-01' THEN consumption END) -
     AVG(CASE WHEN date < '2024-01-01' THEN consumption END)) /
     NULLIF(AVG(CASE WHEN date < '2024-01-01' THEN consumption END), 0) * 100 as growth_pct
FROM sberindex.consumption_municipality
WHERE category = 'Все категории'
GROUP BY territory_id, category
HAVING AVG(CASE WHEN date < '2024-01-01' THEN consumption END) IS NOT NULL
ORDER BY growth_pct DESC NULLS LAST
LIMIT 20;
```

---

## 📝 Источник и лицензия

**Источник**: СберИндекс (Лаборатория)
**Лицензия**: CC BY-SA 4.0 (Creative Commons Attribution-ShareAlike 4.0 International)
**URL**: https://sberindex.ru/ru/research/data-sense-opisanie-nabora-dannikh-khakatona-sberindeksa-po-munitsipalnim-dannim

### Цитирование:

**RU**: Потребительские безналичные расходы на уровне муниципальных образований по категориям трат. СберИндекс. Данные доступны по адресу https://sberindex.ru/ru/research/data-sense-opisanie-nabora-dannikh-khakatona-sberindeksa-po-munitsipalnim-dannim (данные скачаны 2025-10-24).

**EN**: Consumer spending at the municipal level. Sberindex. Available at https://sberindex.ru/ru/research/data-sense-opisanie-nabora-dannikh-khakatona-sberindeksa-po-munitsipalnim-dannim (data downloaded on 2025-10-24).

---

## 🎯 Применение для аналитики

### Возможные исследования:

1. **Региональные различия** - Сравнение потребительского поведения между регионами
2. **Временные тренды** - Выявление сезонности и долгосрочных трендов
3. **Влияние доступности** - Связь между индексом доступности рынков и уровнем потребления
4. **Категориальный анализ** - Структура потребления, приоритеты трат
5. **Экономическое неравенство** - Разброс показателей между МО
6. **Прогнозирование** - Модели предсказания потребления
7. **Пространственный анализ** - Географические паттерны (при наличии координат)

---

**Дата загрузки**: 2025-10-24
**Статус**: ✅ Загрузка завершена успешно
