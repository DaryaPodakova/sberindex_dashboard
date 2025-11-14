#!/usr/bin/env python3
"""
Скрипт 08: Индекс привлекательности v2 (финальный)

Цель: Объединить POAD показатели + транспортную доступность + тип инфраструктуры
Формула:
    Attractiveness_v2 = (
        0.60 * POAD_composite_normalized +
        0.30 * Accessibility_Score +
        0.10 * Infrastructure_Type_Score
    )

Результат:
    - attractiveness_v2.csv — финальный индекс для 128 НП
    - comparison_v1_vs_v2.csv — сравнение v1 и v2
"""

import pandas as pd
import numpy as np
import json
from pathlib import Path
import warnings

warnings.filterwarnings('ignore')


def calculate_infrastructure_score(settlement_type):
    """
    Оценка типа инфраструктуры НП

    Args:
        settlement_type: тип поселения (пгт, село, посёлок, рп, др.)

    Returns:
        score 0-1 (выше для более развитой инфраструктуры)
    """
    # Эвристика: ПГТ/рабочий посёлок имеют лучшую инфраструктуру
    type_lower = str(settlement_type).lower()

    if 'пгт' in type_lower or 'рабочий' in type_lower or 'рп' in type_lower:
        return 1.0
    elif 'посёлок' in type_lower or 'поселок' in type_lower:
        return 0.85
    elif 'город' in type_lower or 'гп' in type_lower:
        return 1.0
    elif 'село' in type_lower:
        return 0.7
    else:
        return 0.6  # другие типы


def normalize_to_01(series):
    """Min-max нормализация в диапазон [0, 1]"""
    min_val = series.min()
    max_val = series.max()

    if max_val == min_val:
        return pd.Series([0.5] * len(series), index=series.index)

    return (series - min_val) / (max_val - min_val)


def run_attractiveness_v2_calculation():
    """Основная функция расчёта индекса привлекательности v2"""

    # Пути к файлам
    script_dir = Path(__file__).parent
    data_dir = script_dir.parent / 'results' / 'data'
    analysis_dir = script_dir.parent / 'results' / 'analysis'

    # =============================
    # 1. ЗАГРУЗКА ДАННЫХ
    # =============================
    print("📂 Загрузка данных...")

    # Attractiveness v1 (POAD composite)
    attractiveness_v1_file = data_dir / 'attractiveness_v1.csv'
    if not attractiveness_v1_file.exists():
        print(f"   ❌ Файл не найден: {attractiveness_v1_file}")
        print("   Сначала запустите 03_attractiveness_v1.py")
        return

    attractiveness_v1_df = pd.read_csv(attractiveness_v1_file)
    print(f"   ✅ Attractiveness v1: {len(attractiveness_v1_df)} НП")

    # Accessibility scores
    accessibility_file = data_dir / 'accessibility_scores.csv'
    if not accessibility_file.exists():
        print(f"   ❌ Файл не найден: {accessibility_file}")
        print("   Сначала запустите 07_accessibility.py")
        return

    accessibility_df = pd.read_csv(accessibility_file)
    print(f"   ✅ Accessibility scores: {len(accessibility_df)} НП")

    # Settlements (для типа инфраструктуры)
    settlements_file = data_dir / 'settlements_with_indicators.csv'
    settlements_df = pd.read_csv(settlements_file)
    print(f"   ✅ Данные НП: {len(settlements_df)} НП")

    # Clusters (для анализа по типам)
    clusters_file = data_dir / 'clusters.csv'
    clusters_df = pd.read_csv(clusters_file)
    print(f"   ✅ Кластеры: {len(clusters_df)} НП")

    # =============================
    # 2. ПОДГОТОВКА ДАННЫХ
    # =============================
    print("\n🔧 Подготовка данных для расчёта v2...")

    # Объединяем все данные
    df = attractiveness_v1_df.copy()

    # Добавляем accessibility_score
    df = df.merge(
        accessibility_df[['settlement_id', 'accessibility_score', 'distance_to_hub_km']],
        on='settlement_id',
        how='left'
    )

    # settlement_type, region_name, population, is_arctic уже есть в attractiveness_v1
    # Просто проверяем, что они есть
    if 'settlement_type' not in df.columns:
        print("   ⚠️  settlement_type не найден, добавляем из settlements_df")
        df = df.merge(
            settlements_df[['settlement_id', 'settlement_type']],
            on='settlement_id',
            how='left'
        )

    df = df.merge(
        clusters_df[['settlement_id', 'cluster_name']],
        on='settlement_id',
        how='left'
    )

    print(f"   ✅ Объединено {len(df)} НП")

    # =============================
    # 3. РАСЧЁТ КОМПОНЕНТОВ ИНДЕКСА
    # =============================
    print("\n📊 Расчёт компонентов индекса v2...")

    # 3.1. POAD Composite (нормализуем attractiveness_v1)
    df['poad_normalized'] = normalize_to_01(df['attractiveness_v1'])
    print(f"   ✅ POAD component (нормализован 0-1)")

    # 3.2. Accessibility Score (уже нормализован 0-1)
    df['accessibility_normalized'] = df['accessibility_score']
    print(f"   ✅ Accessibility component (уже нормализован)")

    # 3.3. Infrastructure Type Score
    df['infrastructure_score'] = df['settlement_type'].apply(calculate_infrastructure_score)
    print(f"   ✅ Infrastructure component (на основе типа НП)")

    # =============================
    # 4. ФИНАЛЬНЫЙ ИНДЕКС V2
    # =============================
    print("\n🎯 Расчёт финального индекса привлекательности v2...")

    # Веса компонентов
    WEIGHT_POAD = 0.60
    WEIGHT_ACCESSIBILITY = 0.30
    WEIGHT_INFRASTRUCTURE = 0.10

    df['attractiveness_v2_score'] = (
        WEIGHT_POAD * df['poad_normalized'] +
        WEIGHT_ACCESSIBILITY * df['accessibility_normalized'] +
        WEIGHT_INFRASTRUCTURE * df['infrastructure_score']
    )

    print(f"   ✅ Индекс v2 рассчитан (веса: POAD={WEIGHT_POAD}, Access={WEIGHT_ACCESSIBILITY}, Infra={WEIGHT_INFRASTRUCTURE})")

    # Нормализуем финальный индекс 0-10 для удобства интерпретации
    df['attractiveness_v2_score_0_10'] = df['attractiveness_v2_score'] * 10

    # =============================
    # 5. АНАЛИЗ РЕЗУЛЬТАТОВ
    # =============================
    print("\n📊 Статистика индекса v2:")
    print("=" * 80)

    stats = {
        'mean_v2': float(df['attractiveness_v2_score'].mean()),
        'median_v2': float(df['attractiveness_v2_score'].median()),
        'min_v2': float(df['attractiveness_v2_score'].min()),
        'max_v2': float(df['attractiveness_v2_score'].max()),
        'std_v2': float(df['attractiveness_v2_score'].std())
    }

    print(f"   • Среднее значение (0-1): {stats['mean_v2']:.3f}")
    print(f"   • Медиана (0-1): {stats['median_v2']:.3f}")
    print(f"   • Диапазон (0-1): {stats['min_v2']:.3f} - {stats['max_v2']:.3f}")
    print(f"   • Стандартное отклонение: {stats['std_v2']:.3f}")

    print(f"\n   • Среднее значение (0-10): {stats['mean_v2']*10:.2f}")

    # Топ-10 НП по индексу v2
    print(f"\n   🏆 ТОП-10 НП по индексу привлекательности v2:")
    top_10_v2 = df.nlargest(10, 'attractiveness_v2_score')
    for i, (_, row) in enumerate(top_10_v2.iterrows(), 1):
        print(f"      {i}. {row['settlement_name']}: v2={row['attractiveness_v2_score_0_10']:.2f}/10 "
              f"(POAD={row['poad_normalized']:.2f}, Access={row['accessibility_normalized']:.2f}, "
              f"Infra={row['infrastructure_score']:.2f})")

    # Худшие 10 НП
    print(f"\n   ⚠️  10 НП с наименьшей привлекательностью:")
    bottom_10_v2 = df.nsmallest(10, 'attractiveness_v2_score')
    for i, (_, row) in enumerate(bottom_10_v2.iterrows(), 1):
        print(f"      {i}. {row['settlement_name']}: v2={row['attractiveness_v2_score_0_10']:.2f}/10 "
              f"(население={row['population']:.0f}, кластер={row['cluster_name']})")

    print("=" * 80)

    # =============================
    # 6. СРАВНЕНИЕ V1 vs V2
    # =============================
    print("\n🔄 Сравнение индекса v1 vs v2...")

    df['v1_normalized'] = normalize_to_01(df['attractiveness_v1'])
    df['delta_v2_minus_v1'] = df['attractiveness_v2_score'] - df['v1_normalized']
    df['rank_v1'] = df['attractiveness_v1'].rank(ascending=False, method='min').astype(int)
    df['rank_v2'] = df['attractiveness_v2_score'].rank(ascending=False, method='min').astype(int)
    df['rank_change'] = df['rank_v1'] - df['rank_v2']

    # Топ-10 НП с наибольшим улучшением (v2 > v1)
    print(f"\n   📈 ТОП-10 НП с наибольшим улучшением (v2 > v1):")
    top_improved = df.nlargest(10, 'delta_v2_minus_v1')
    for i, (_, row) in enumerate(top_improved.iterrows(), 1):
        print(f"      {i}. {row['settlement_name']}: Δ={row['delta_v2_minus_v1']:.3f}, "
              f"rank: {row['rank_v1']}→{row['rank_v2']} (доступность={row['accessibility_normalized']:.2f})")

    # Топ-10 НП с наибольшим ухудшением (v2 < v1)
    print(f"\n   📉 ТОП-10 НП с наибольшим ухудшением (v2 < v1):")
    top_worsened = df.nsmallest(10, 'delta_v2_minus_v1')
    for i, (_, row) in enumerate(top_worsened.iterrows(), 1):
        print(f"      {i}. {row['settlement_name']}: Δ={row['delta_v2_minus_v1']:.3f}, "
              f"rank: {row['rank_v1']}→{row['rank_v2']} (доступность={row['accessibility_normalized']:.2f})")

    # =============================
    # 7. СОХРАНЕНИЕ РЕЗУЛЬТАТОВ
    # =============================
    print("\n💾 Сохранение результатов...")

    # 7.1. Attractiveness v2 (основной файл)
    output_columns = [
        'settlement_id', 'settlement_name', 'region_name', 'population', 'is_arctic',
        'cluster_name', 'attractiveness_v2_score', 'attractiveness_v2_score_0_10',
        'poad_normalized', 'accessibility_normalized', 'infrastructure_score',
        'distance_to_hub_km', 'rank_v2'
    ]

    attractiveness_v2_file = data_dir / 'attractiveness_v2.csv'
    df[output_columns].to_csv(attractiveness_v2_file, index=False)
    print(f"   ✅ {attractiveness_v2_file}")
    print(f"   📊 Размер: {attractiveness_v2_file.stat().st_size / 1024:.1f} KB")

    # 7.2. Comparison v1 vs v2
    comparison_columns = [
        'settlement_id', 'settlement_name', 'attractiveness_v1', 'attractiveness_v2_score',
        'v1_normalized', 'delta_v2_minus_v1', 'rank_v1', 'rank_v2', 'rank_change'
    ]

    comparison_file = data_dir / 'comparison_v1_vs_v2.csv'
    df[comparison_columns].to_csv(comparison_file, index=False)
    print(f"   ✅ {comparison_file}")

    # 7.3. Статистика (JSON)
    stats['weight_poad'] = WEIGHT_POAD
    stats['weight_accessibility'] = WEIGHT_ACCESSIBILITY
    stats['weight_infrastructure'] = WEIGHT_INFRASTRUCTURE

    stats_file = analysis_dir / 'attractiveness_v2_stats.json'
    with open(stats_file, 'w', encoding='utf-8') as f:
        json.dump(stats, f, ensure_ascii=False, indent=2)
    print(f"   ✅ {stats_file}")

    # =============================
    # 8. ИТОГОВАЯ СВОДКА
    # =============================
    print("\n" + "=" * 80)
    print("✅ ИНДЕКС ПРИВЛЕКАТЕЛЬНОСТИ V2 ЗАВЕРШЁН")
    print("=" * 80)
    print(f"📊 Обработано НП: {len(df)}")
    print(f"📊 Средний индекс v2 (0-10): {stats['mean_v2']*10:.2f}")
    print(f"📊 Компоненты: POAD (60%) + Accessibility (30%) + Infrastructure (10%)")
    print(f"📂 Результаты сохранены в: {data_dir}")
    print("=" * 80)

    return df, stats


if __name__ == '__main__':
    run_attractiveness_v2_calculation()
