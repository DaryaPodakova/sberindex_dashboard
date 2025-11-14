#!/usr/bin/env python3
"""
Скрипт 01: Загрузка данных из PostgreSQL (схема sberindex)

Цель: Извлечь все данные POAD для 128 населённых пунктов
Результат: results/data/settlements_raw.csv, indicators_raw.csv, settlements_with_indicators.csv
"""

import os
import sys
import pandas as pd
from pathlib import Path
from dotenv import load_dotenv

# Добавляем корневую папку проекта в PYTHONPATH
project_root = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(project_root))

from db_tools.connection_pool_legacy import DatabaseManager


def load_data():
    """Загрузка данных из схемы sberindex"""

    # Загружаем переменные окружения
    env_path = project_root / '.env'
    load_dotenv(env_path)

    # Параметры подключения к БД
    db_params = {
        'host': os.getenv('DB_HOST', 'host.docker.internal'),
        'port': os.getenv('DB_PORT', '5432'),
        'database': os.getenv('DB_NAME', 'platform'),
        'user': os.getenv('DB_USER', 'bot_etl_user2'),
        'password': os.getenv('DB_PASSWORD', 'allen'),
    }

    print(f"🔗 Подключение к БД: {db_params['host']}:{db_params['port']}/{db_params['database']}")

    db = DatabaseManager(db_params)

    # Папка для результатов
    results_dir = Path(__file__).parent.parent / 'results' / 'data'
    results_dir.mkdir(parents=True, exist_ok=True)

    def extract_data(cur):
        """Извлечение данных из БД"""
        cur.execute('SET search_path TO sberindex;')

        # 1. Загрузка справочника населённых пунктов
        print("\n📍 Загрузка dict_settlements...")
        query_settlements = """
        SELECT
            s.settlement_id,
            s.settlement_name,
            r.region_name,
            m_up.municipality_name AS municipality_up_name,
            m_down.municipality_name AS municipality_down_name,
            s.settlement_type,
            COALESCE(a.is_arctic, FALSE) AS is_arctic
        FROM dict_settlements s
        LEFT JOIN dict_regions r ON s.region_id = r.region_id
        LEFT JOIN dict_municipalities m_up ON s.municipality_up_id = m_up.municipality_id
        LEFT JOIN dict_municipalities m_down ON s.municipality_down_id = m_down.municipality_id
        LEFT JOIN meta_settlement_attributes a ON s.settlement_id = a.settlement_id
        ORDER BY s.settlement_id;
        """
        cur.execute(query_settlements)
        settlements_data = cur.fetchall()
        settlements_columns = [desc[0] for desc in cur.description]
        df_settlements = pd.DataFrame(settlements_data, columns=settlements_columns)

        print(f"   ✅ Загружено {len(df_settlements)} населённых пунктов")

        # Загрузка населения (последний год)
        print("\n👥 Загрузка meta_settlement_population...")
        query_population = """
        WITH latest_population AS (
            SELECT
                settlement_id,
                year,
                population_total,
                ROW_NUMBER() OVER (PARTITION BY settlement_id ORDER BY year DESC) as rn
            FROM meta_settlement_population
        )
        SELECT settlement_id, population_total AS population
        FROM latest_population
        WHERE rn = 1;
        """
        cur.execute(query_population)
        population_data = cur.fetchall()
        population_columns = [desc[0] for desc in cur.description]
        df_population = pd.DataFrame(population_data, columns=population_columns)

        # Объединяем с settlements
        df_settlements = df_settlements.merge(df_population, on='settlement_id', how='left')

        print(f"   ✅ Загружено населения для {df_population['settlement_id'].nunique()} НП")

        # 2. Загрузка координат
        print("\n🗺️  Загрузка meta_settlement_coordinates...")
        query_coords = """
        SELECT
            settlement_id,
            latitude,
            longitude
        FROM meta_settlement_coordinates
        WHERE latitude IS NOT NULL AND longitude IS NOT NULL
        ORDER BY settlement_id;
        """
        cur.execute(query_coords)
        coords_data = cur.fetchall()
        coords_columns = [desc[0] for desc in cur.description]
        df_coords = pd.DataFrame(coords_data, columns=coords_columns)

        print(f"   ✅ Загружено координат для {len(df_coords)} НП")

        # 3. Загрузка показателей POAD
        print("\n📊 Загрузка fact_settlement_indicators...")
        query_indicators = """
        SELECT
            f.settlement_id,
            COALESCE(d.indicator_number, 'IND_' || f.indicator_id::TEXT) AS indicator_code,
            f.value_numeric AS indicator_value,
            f.year,
            d.indicator_name
        FROM fact_settlement_indicators f
        LEFT JOIN dict_indicators d ON f.indicator_id = d.indicator_id
        WHERE f.value_numeric IS NOT NULL
        ORDER BY f.settlement_id, f.indicator_id;
        """
        cur.execute(query_indicators)
        indicators_data = cur.fetchall()
        indicators_columns = [desc[0] for desc in cur.description]
        df_indicators = pd.DataFrame(indicators_data, columns=indicators_columns)

        print(f"   ✅ Загружено {len(df_indicators):,} фактов POAD")
        print(f"   ℹ️  Уникальных показателей: {df_indicators['indicator_code'].nunique()}")
        print(f"   ℹ️  Уникальных НП: {df_indicators['settlement_id'].nunique()}")

        # 4. Загрузка справочника показателей
        print("\n📋 Загрузка dict_indicators...")
        query_dict_indicators = """
        SELECT
            indicator_number AS indicator_code,
            indicator_name,
            acronym,
            calculation_method,
            normalization_method
        FROM dict_indicators
        WHERE is_active = TRUE
        ORDER BY indicator_number;
        """
        cur.execute(query_dict_indicators)
        dict_indicators_data = cur.fetchall()
        dict_indicators_columns = [desc[0] for desc in cur.description]
        df_dict_indicators = pd.DataFrame(dict_indicators_data, columns=dict_indicators_columns)

        print(f"   ✅ Загружено {len(df_dict_indicators)} описаний показателей")

        # Объединяем данные
        print("\n🔗 Объединение данных...")

        # Settlements + Coordinates
        df_full = df_settlements.merge(df_coords, on='settlement_id', how='left')

        print(f"   ✅ Объединено: {len(df_full)} НП с координатами")

        # Сохраняем результаты
        print("\n💾 Сохранение результатов...")

        settlements_file = results_dir / 'settlements_raw.csv'
        df_full.to_csv(settlements_file, index=False, encoding='utf-8')
        print(f"   ✅ {settlements_file}")

        indicators_file = results_dir / 'indicators_raw.csv'
        df_indicators.to_csv(indicators_file, index=False, encoding='utf-8')
        print(f"   ✅ {indicators_file}")

        dict_indicators_file = results_dir / 'dict_indicators.csv'
        df_dict_indicators.to_csv(dict_indicators_file, index=False, encoding='utf-8')
        print(f"   ✅ {dict_indicators_file}")

        # Pivot таблица: НП × Показатели (для корреляционного анализа)
        print("\n🔄 Создание pivot таблицы (НП × Показатели)...")
        df_pivot = df_indicators.pivot_table(
            index='settlement_id',
            columns='indicator_code',
            values='indicator_value',
            aggfunc='first'  # Берём первое значение (если несколько годов)
        ).reset_index()

        # Объединяем с информацией о НП
        df_pivot_full = df_full.merge(df_pivot, on='settlement_id', how='left')

        pivot_file = results_dir / 'settlements_with_indicators.csv'
        df_pivot_full.to_csv(pivot_file, index=False, encoding='utf-8')
        print(f"   ✅ {pivot_file}")
        print(f"   ℹ️  Размерность: {df_pivot_full.shape[0]} НП × {df_pivot_full.shape[1]} колонок")

        # Статистика
        print("\n📈 СТАТИСТИКА:")
        print(f"   • Всего НП: {len(df_full)}")
        print(f"   • НП с координатами: {df_full['latitude'].notna().sum()}")
        print(f"   • Арктических НП: {df_full['is_arctic'].sum()}")
        print(f"   • Не арктических НП: {(~df_full['is_arctic']).sum()}")
        print(f"   • Фактов POAD: {len(df_indicators):,}")
        print(f"   • Уникальных показателей: {df_indicators['indicator_code'].nunique()}")
        print(f"   • Пропусков в pivot: {df_pivot_full.isna().sum().sum():,}")

        # Проверка на пропуски по колонкам
        missing_pct = (df_pivot_full.isna().sum() / len(df_pivot_full) * 100).sort_values(ascending=False)
        indicators_with_missing = missing_pct[missing_pct > 0]

        if len(indicators_with_missing) > 0:
            print(f"\n⚠️  Показатели с пропусками (топ-10):")
            for idx, pct in indicators_with_missing.head(10).items():
                print(f"      {idx}: {pct:.1f}% пропусков")
        else:
            print("\n✅ Нет пропусков в данных!")

        return 0

    # Выполнение
    db.execute_with_retry(extract_data)
    db.close()

    print("\n✅ ЗАГРУЗКА ЗАВЕРШЕНА!")
    print(f"📂 Результаты сохранены в: {results_dir}")


if __name__ == '__main__':
    load_data()
