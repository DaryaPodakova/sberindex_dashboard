# Документация Arctic NDI Dashboard

Эта директория содержит документацию по Northern Development Index (NDI) для арктических населённых пунктов.

## Файлы документации

### 1. [methodology_ndi.md](methodology_ndi.md)
**Методология расчёта NDI**

Подробное описание:
- Формула NDI и весовые коэффициенты
- 6 компонентов индекса (POAD, Market Access, Consumption, Accessibility, Climate, Mobility)
- Pipeline обработки данных
- Топ-10 показателей POAD
- Диаграмма формирования данных

### 2. [REPRODUCE_NDI.md](REPRODUCE_NDI.md)
**Инструкция по воспроизводимости расчётов**

Пошаговая инструкция:
- Быстрый старт (использование готовых результатов)
- Полная воспроизводимость (пересчёт из компонентов)
- Работа с PostgreSQL
- Примеры анализа (корреляция, кластеризация)
- Проверка целостности данных

### 3. [data_README.md](data_README.md)
**Описание структуры данных**

Справочная информация:
- Описание всех CSV файлов
- Структура компонентов NDI
- Справочники (settlements, regions, municipalities)
- Источники данных
- Валидация и тесты

## Расположение файлов проекта

```
pet_project/
├── sberbank_hackaton_11_2025/           # Проект хакатона
│   ├── scripts/                          # Скрипты обработки данных
│   │   ├── 01_load_data.py              # Загрузка POAD
│   │   ├── 02_correlation_analysis.py   # Корреляционный анализ
│   │   ├── 03_attractiveness_v1.py      # Расчёт POAD индекса
│   │   ├── 06_distances.py              # Матрица расстояний
│   │   ├── 07_accessibility.py          # Accessibility score
│   │   ├── 09_fetch_climate_data.py     # Загрузка климата
│   │   ├── 10_load_climate_to_postgres.py # Расчёт HDD
│   │   ├── create_ndi_view.sql          # ФИНАЛЬНЫЙ скрипт NDI
│   │   └── export_ndi_data.py           # Экспорт всех данных
│   └── arctic/
│       ├── data/                         # Экспортированные CSV
│       │   ├── ndi_scores.csv           # ИТОГОВЫЙ NDI (128 НП)
│       │   ├── components/               # 6 компонентов
│       │   └── dictionaries/             # Справочники
│       └── etl_load_mobility_index.py   # Загрузка мобильности
│
└── sberindex_dashboard/                  # Dashboard проект
    ├── docs/                             # Документация (ВЫ ЗДЕСЬ)
    │   ├── README.md                    # Этот файл
    │   ├── methodology_ndi.md           # Методология
    │   ├── REPRODUCE_NDI.md             # Воспроизводимость
    │   └── data_README.md               # Описание данных
    ├── sql/                              # SQL скрипты
    │   └── create_ndi_view.sql          # NDI VIEW (22 КБ, 390 строк)
    └── widgets/                          # Виджеты дашборда
```

## Быстрый старт

### Использовать готовый NDI

```python
import pandas as pd

# Загрузить итоговый NDI
ndi = pd.read_csv('../sberbank_hackaton_11_2025/arctic/data/ndi_scores.csv')

# Топ-10 по NDI
print(ndi.head(10)[['settlement_name', 'region_name', 'ndi_score_100']])
```

### Воспроизвести расчёт

```bash
# Вариант 1: Использовать локальный SQL скрипт
psql -U bot_etl_user2 -d platform -f ../sql/create_ndi_view.sql

# Вариант 2: Из директории хакатона
cd ../sberbank_hackaton_11_2025
psql -U bot_etl_user2 -d platform -f scripts/create_ndi_view.sql
python arctic/scripts/export_ndi_data.py
```

## Ссылки

### В sberindex_dashboard:
- **SQL View:** `../sql/create_ndi_view.sql` (локальная копия)
- **Документация:** `./` (эта директория)

### В sberbank_hackaton_11_2025:
- **Данные:** `../sberbank_hackaton_11_2025/arctic/data/`
- **Скрипты обработки:** `../sberbank_hackaton_11_2025/scripts/`
- **SQL View (исходный):** `../sberbank_hackaton_11_2025/scripts/create_ndi_view.sql`

## Контакты

Вопросы по методологии и данным: daria@example.com

---

Последнее обновление: 2025-11-14
