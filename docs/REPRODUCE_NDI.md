# 🔁 Инструкция по воспроизводимости NDI

## Быстрый старт (5 минут)

### ✅ **Вариант 1: Использовать готовый результат**

```python
import pandas as pd

# Загрузить итоговый NDI (128 арктических НП)
ndi = pd.read_csv('data/ndi_scores.csv')

# Посмотреть топ-10 по NDI
print(ndi.head(10)[['settlement_name', 'region_name', 'ndi_score_100', 'ndi_rank']])

# Найти худшие территории (зона риска)
print(ndi[ndi['ndi_score_100'] < 40][['settlement_name', 'ndi_score_100']])
```

**Выход:**
```
   settlement_name          region_name  ndi_score_100  ndi_rank
0  Мурманск                Мурманская обл      78.5         1
1  Апатиты                 Мурманская обл      72.1         2
2  Северодвинск            Архангельская       69.8         3
...
```

---

## 🔧 Полная воспроизводимость (30 минут)

### **Шаг 1: Проверить структуру данных**

```bash
# Все данные в директории arctic/data/
cd sberbank_hackaton_11_2025/arctic/data

# Структура:
# ├── ndi_scores.csv              # ИТОГОВЫЙ РЕЗУЛЬТАТ
# ├── components/                  # 6 компонентов NDI
# │   ├── poad_attractiveness_v1.csv
# │   ├── market_access_municipality.csv
# │   ├── consumption_municipality_2024.csv
# │   ├── accessibility_scores.csv
# │   ├── climate_monthly_2023_2024.csv
# │   ├── climate_yearly_hdd.csv
# │   └── mobility_index_municipality.csv
# ├── dictionaries/                # Справочники
# │   ├── dict_settlements.csv
# │   ├── dict_regions.csv
# │   ├── dict_municipalities.csv
# │   ├── dict_indicators.csv
# │   └── meta_settlement_attributes.csv
# └── README.md                    # Документация
```

---

### **Шаг 2: Загрузить данные в Python**

```python
import pandas as pd
from pathlib import Path

# Пути
DATA_DIR = Path('data')

# 1. Итоговый NDI
ndi = pd.read_csv(DATA_DIR / 'ndi_scores.csv')
print(f"✅ NDI scores: {len(ndi)} settlements")

# 2. Компоненты
poad = pd.read_csv(DATA_DIR / 'components' / 'poad_attractiveness_v1.csv')
market = pd.read_csv(DATA_DIR / 'components' / 'market_access_municipality.csv')
consumption = pd.read_csv(DATA_DIR / 'components' / 'consumption_municipality_2024.csv')
accessibility = pd.read_csv(DATA_DIR / 'components' / 'accessibility_scores.csv')
climate_monthly = pd.read_csv(DATA_DIR / 'components' / 'climate_monthly_2023_2024.csv')
climate_yearly = pd.read_csv(DATA_DIR / 'components' / 'climate_yearly_hdd.csv')
mobility = pd.read_csv(DATA_DIR / 'components' / 'mobility_index_municipality.csv')

print(f"✅ POAD: {len(poad)} settlements")
print(f"✅ Market Access: {len(market)} municipalities")
print(f"✅ Consumption: {len(consumption)} records")
print(f"✅ Climate (monthly): {len(climate_monthly)} records")
print(f"✅ Climate (yearly): {len(climate_yearly)} records")
print(f"✅ Mobility: {len(mobility)} municipalities")

# 3. Справочники
settlements = pd.read_csv(DATA_DIR / 'dictionaries' / 'dict_settlements.csv')
regions = pd.read_csv(DATA_DIR / 'dictionaries' / 'dict_regions.csv')
municipalities = pd.read_csv(DATA_DIR / 'dictionaries' / 'dict_municipalities.csv')
indicators = pd.read_csv(DATA_DIR / 'dictionaries' / 'dict_indicators.csv')

print(f"✅ Settlements: {len(settlements)}")
print(f"✅ Regions: {len(regions)}")
print(f"✅ Indicators (POAD): {len(indicators)}")
```

---

### **Шаг 3: Валидировать формулу NDI**

```python
# Формула NDI:
# NDI = 0.35×POAD + 0.20×Market + 0.15×Consumption +
#       0.15×Access + 0.10×Climate + 0.05×Mobility

# Пример валидации для первого НП
row = ndi.iloc[0]

ndi_recalc = (
    0.35 * row['poad_score'] +
    0.20 * row['market_score'] +
    0.15 * row['consumption_score'] +
    0.15 * row['accessibility_score'] +
    0.10 * row['climate_score'] +
    0.05 * row['mobility_score']
)

print(f"NDI из файла: {row['ndi_score']:.4f}")
print(f"NDI пересчёт:  {ndi_recalc:.4f}")
print(f"Совпадение: {abs(row['ndi_score'] - ndi_recalc) < 0.0001}")
```

---

### **Шаг 4: Воспроизвести полный расчёт**

```python
# Полный алгоритм расчёта NDI описан в:
# scripts/create_ndi_view.sql (строки 1-390)

# Ключевые шаги:
# 1. Нормализация компонентов (min-max scaling 0-1)
# 2. Заполнение пропусков (региональные средние)
# 3. Weighted sum с весами (0.35, 0.20, 0.15, 0.15, 0.10, 0.05)
# 4. Ранжирование по ndi_score

# Пример нормализации (POAD уже нормализован):
from sklearn.preprocessing import MinMaxScaler

def normalize(df, column):
    scaler = MinMaxScaler()
    return scaler.fit_transform(df[[column]])

# Применить к каждому компоненту
# (детальная реализация см. в SQL-view)
```

---

## 🗄️ Вариант с PostgreSQL

### **Шаг 1: Загрузить данные в БД**

```bash
# Создать схему
psql -U bot_etl_user2 -d platform -c "CREATE SCHEMA IF NOT EXISTS sberindex;"

# Загрузить все CSV через утилиту
python scripts/load_csv_to_postgres.py
```

### **Шаг 2: Создать VIEW**

```bash
# Выполнить SQL скрипт создания VIEW
psql -U bot_etl_user2 -d platform -f scripts/create_ndi_view.sql
```

### **Шаг 3: Запросить данные**

```sql
-- Топ-10 по NDI
SELECT settlement_name, region_name, ndi_score_100, ndi_rank
FROM sberindex.vw_ndi_calculation
ORDER BY ndi_score_100 DESC LIMIT 10;

-- НП в зоне риска (NDI < 4.0)
SELECT settlement_name, region_name, ndi_score_100,
       poad_score_100, climate_score_100
FROM sberindex.vw_ndi_calculation
WHERE ndi_score_100 < 40
ORDER BY ndi_score_100 ASC;

-- Статистика по регионам
SELECT region_name, COUNT(*) as settlements, ROUND(AVG(ndi_score_100), 2) as avg_ndi
FROM sberindex.vw_ndi_calculation
GROUP BY region_name
ORDER BY avg_ndi DESC;
```

---

## 📊 Примеры анализа

### **1. Корреляция компонентов**

```python
import seaborn as sns
import matplotlib.pyplot as plt

# Корреляционная матрица компонентов NDI
components = ['poad_score_100', 'market_score_100', 'consumption_score_100',
              'accessibility_score_100', 'climate_score_100', 'mobility_score_100']

corr = ndi[components].corr()

sns.heatmap(corr, annot=True, cmap='coolwarm', center=0)
plt.title('Корреляция компонентов NDI')
plt.show()
```

### **2. Декомпозиция NDI**

```python
# Какие компоненты отличают топ-10 от аутсайдеров?

top_10 = ndi.head(10)
bottom_10 = ndi.tail(10)

comparison = pd.DataFrame({
    'Топ-10': top_10[components].mean(),
    'Аутсайдеры': bottom_10[components].mean()
})

comparison['Разница'] = comparison['Топ-10'] - comparison['Аутсайдеры']
print(comparison.sort_values('Разница', ascending=False))
```

### **3. Кластеризация поселений**

```python
from sklearn.cluster import KMeans

# Кластеризация по компонентам NDI (k=4)
X = ndi[components].fillna(ndi[components].mean())
kmeans = KMeans(n_clusters=4, random_state=42)
ndi['cluster'] = kmeans.fit_predict(X)

# Характеристика кластеров
for i in range(4):
    cluster_data = ndi[ndi['cluster'] == i]
    print(f"\n🔹 Кластер {i+1}: {len(cluster_data)} НП")
    print(f"   Средний NDI: {cluster_data['ndi_score_100'].mean():.1f}")
    print(f"   Примеры: {', '.join(cluster_data['settlement_name'].head(3))}")
```

---

## ✅ Проверка целостности

```bash
# 1. Количество строк (должно быть 128 НП + 1 заголовок)
wc -l data/ndi_scores.csv  # 129

# 2. Нет пропусков в NDI
python -c "import pandas as pd; df = pd.read_csv('data/ndi_scores.csv'); print(df['ndi_score_100'].isna().sum())"  # 0

# 3. Диапазон NDI (0-100)
python -c "import pandas as pd; df = pd.read_csv('data/ndi_scores.csv'); print(f'Min: {df[\"ndi_score_100\"].min()}, Max: {df[\"ndi_score_100\"].max()}')"
```

---

## 📚 Дополнительно

- **Документация данных:** [data/README.md](data/README.md)
- **SQL-скрипт VIEW:** [scripts/12_create_ndi_view.sql](scripts/12_create_ndi_view.sql)
- **Экспорт данных:** [scripts/export_ndi_data.py](scripts/export_ndi_data.py)

---

## 🤝 Поддержка

Если данные не воспроизводятся:
1. Проверьте версию Python (≥3.8) и pandas (≥1.3)
2. Убедитесь, что все CSV файлы в директории `data/`
3. Откройте Issue в репозитории с описанием проблемы

---

**Данные готовы для воспроизводимости исследования! 🚀**
