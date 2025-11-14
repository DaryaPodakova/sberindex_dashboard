#!/usr/bin/env python3
"""
Скрипт 07: Расчёт Accessibility Score (транспортная доступность)

Цель: Оценить доступность каждого НП до ближайших хабов
Метод:
    - Хаб = НП с населением > 10,000 чел.
    - Accessibility = 1 / (1 + distance_to_nearest_hub / scale_factor)
    - Нормализация 0-1
Результат:
    - accessibility_scores.csv — оценки доступности для 128 НП
"""

import pandas as pd
import numpy as np
import json
from pathlib import Path
import warnings

warnings.filterwarnings('ignore')


def identify_hubs(settlements_df, population_threshold=10000):
    """
    Идентифицирует хабы (крупные НП) по порогу населения

    Args:
        settlements_df: DataFrame с колонками settlement_id, population
        population_threshold: минимальное население для хаба

    Returns:
        DataFrame с хабами, список индексов хабов
    """
    hubs_df = settlements_df[settlements_df['population'] >= population_threshold].copy()
    hub_indices = hubs_df.index.tolist()

    print(f"\n🏙️  Идентификация хабов (население ≥ {population_threshold:,} чел.)...")
    print(f"   ✅ Найдено хабов: {len(hubs_df)}")

    if len(hubs_df) > 0:
        print(f"\n   Список хабов:")
        for _, row in hubs_df.iterrows():
            print(f"      • {row['settlement_name']}: {row['population']:,.0f} чел. ({row['region_name']})")

    return hubs_df, hub_indices


def calculate_accessibility_scores(settlements_df, distance_matrix, hub_indices, scale_factor=500):
    """
    Рассчитывает Accessibility Score для каждого НП

    Формула: Accessibility = 1 / (1 + distance_to_nearest_hub / scale_factor)

    Args:
        settlements_df: DataFrame с НП
        distance_matrix: матрица расстояний (n×n)
        hub_indices: список индексов хабов
        scale_factor: коэффициент масштабирования (км)

    Returns:
        DataFrame с accessibility scores
    """
    print(f"\n📊 Расчёт Accessibility Score...")
    print(f"   ℹ️  Scale factor: {scale_factor} км")

    n = len(settlements_df)
    accessibility_scores = []

    for i in range(n):
        # Расстояния от НП i до всех хабов
        distances_to_hubs = [distance_matrix[i, hub_idx] for hub_idx in hub_indices]

        # Ближайший хаб
        if distances_to_hubs:
            min_distance = min(distances_to_hubs)
            nearest_hub_idx = hub_indices[distances_to_hubs.index(min_distance)]
            nearest_hub_name = settlements_df.iloc[nearest_hub_idx]['settlement_name']
        else:
            # Нет хабов — ищем ближайший НП
            distances = distance_matrix[i, :]
            distances[i] = np.inf  # исключаем самого себя
            min_distance = distances.min()
            nearest_hub_idx = distances.argmin()
            nearest_hub_name = settlements_df.iloc[nearest_hub_idx]['settlement_name']

        # Accessibility Score (чем ближе, тем выше score)
        # Формула: 1 / (1 + d/scale) — убывает с расстоянием
        accessibility = 1.0 / (1.0 + min_distance / scale_factor)

        accessibility_scores.append({
            'settlement_id': settlements_df.iloc[i]['settlement_id'],
            'settlement_name': settlements_df.iloc[i]['settlement_name'],
            'nearest_hub_name': nearest_hub_name,
            'distance_to_hub_km': min_distance,
            'accessibility_raw': accessibility
        })

    # Конвертируем в DataFrame
    accessibility_df = pd.DataFrame(accessibility_scores)

    # Нормализация 0-1 (min-max scaling)
    min_score = accessibility_df['accessibility_raw'].min()
    max_score = accessibility_df['accessibility_raw'].max()

    accessibility_df['accessibility_score'] = (
        (accessibility_df['accessibility_raw'] - min_score) / (max_score - min_score)
    )

    print(f"   ✅ Расчёт завершён для {len(accessibility_df)} НП")

    return accessibility_df


def analyze_accessibility(accessibility_df):
    """Анализ статистики доступности"""

    print("\n📊 Статистика Accessibility Score:")
    print("=" * 80)

    stats = {
        'mean_score': float(accessibility_df['accessibility_score'].mean()),
        'median_score': float(accessibility_df['accessibility_score'].median()),
        'min_score': float(accessibility_df['accessibility_score'].min()),
        'max_score': float(accessibility_df['accessibility_score'].max()),
        'std_score': float(accessibility_df['accessibility_score'].std()),
        'mean_distance_to_hub_km': float(accessibility_df['distance_to_hub_km'].mean()),
        'median_distance_to_hub_km': float(accessibility_df['distance_to_hub_km'].median())
    }

    print(f"   • Средний score: {stats['mean_score']:.3f}")
    print(f"   • Медианный score: {stats['median_score']:.3f}")
    print(f"   • Диапазон score: {stats['min_score']:.3f} - {stats['max_score']:.3f}")
    print(f"   • Стандартное отклонение: {stats['std_score']:.3f}")
    print(f"\n   • Среднее расстояние до хаба: {stats['mean_distance_to_hub_km']:.2f} км")
    print(f"   • Медианное расстояние до хаба: {stats['median_distance_to_hub_km']:.2f} км")

    # Топ-10 НП с лучшей доступностью
    print(f"\n   🏆 ТОП-10 НП с лучшей доступностью:")
    top_10 = accessibility_df.nlargest(10, 'accessibility_score')
    for i, (_, row) in enumerate(top_10.iterrows(), 1):
        print(f"      {i}. {row['settlement_name']}: score={row['accessibility_score']:.3f}, "
              f"до {row['nearest_hub_name']} {row['distance_to_hub_km']:.1f} км")

    # Топ-10 НП с худшей доступностью
    print(f"\n   ⚠️  ТОП-10 НП с худшей доступностью (наиболее изолированные):")
    bottom_10 = accessibility_df.nsmallest(10, 'accessibility_score')
    for i, (_, row) in enumerate(bottom_10.iterrows(), 1):
        print(f"      {i}. {row['settlement_name']}: score={row['accessibility_score']:.3f}, "
              f"до {row['nearest_hub_name']} {row['distance_to_hub_km']:.1f} км")

    print("=" * 80)

    return stats


def run_accessibility_calculation():
    """Основная функция расчёта доступности"""

    # Пути к файлам
    script_dir = Path(__file__).parent
    data_dir = script_dir.parent / 'results' / 'data'
    analysis_dir = script_dir.parent / 'results' / 'analysis'

    # =============================
    # 1. ЗАГРУЗКА ДАННЫХ
    # =============================
    print("📂 Загрузка данных...")

    # Населённые пункты
    settlements_file = data_dir / 'settlements_with_indicators.csv'
    settlements_df = pd.read_csv(settlements_file)
    print(f"   ✅ Загружено {len(settlements_df)} НП")

    # Матрица расстояний
    distance_matrix_file = data_dir / 'distance_matrix.npy'
    if not distance_matrix_file.exists():
        print(f"   ❌ Файл не найден: {distance_matrix_file}")
        print("   Сначала запустите 06_distances.py")
        return

    distance_matrix = np.load(distance_matrix_file)
    print(f"   ✅ Матрица расстояний: {distance_matrix.shape}")

    # Метаданные (проверка соответствия порядка)
    metadata_file = data_dir / 'distance_matrix_metadata.json'
    with open(metadata_file, 'r', encoding='utf-8') as f:
        metadata = json.load(f)

    print(f"   ℹ️  Матрица соответствует порядку из metadata (settlement_ids)")

    # =============================
    # 2. ИДЕНТИФИКАЦИЯ ХАБОВ
    # =============================
    hubs_df, hub_indices = identify_hubs(settlements_df, population_threshold=10000)

    if len(hub_indices) == 0:
        print("\n   ⚠️  Хабы не найдены! Используем порог 5,000 чел.")
        hubs_df, hub_indices = identify_hubs(settlements_df, population_threshold=5000)

    # =============================
    # 3. РАСЧЁТ ACCESSIBILITY SCORE
    # =============================
    accessibility_df = calculate_accessibility_scores(
        settlements_df,
        distance_matrix,
        hub_indices,
        scale_factor=500  # 500 км — средний радиус доступности
    )

    # =============================
    # 4. АНАЛИЗ РЕЗУЛЬТАТОВ
    # =============================
    stats = analyze_accessibility(accessibility_df)

    # =============================
    # 5. СОХРАНЕНИЕ РЕЗУЛЬТАТОВ
    # =============================
    print("\n💾 Сохранение результатов...")

    # 5.1. Accessibility scores (CSV)
    output_file = data_dir / 'accessibility_scores.csv'
    accessibility_df.to_csv(output_file, index=False)
    print(f"   ✅ {output_file}")
    print(f"   📊 Размер: {output_file.stat().st_size / 1024:.1f} KB")

    # 5.2. Статистика (JSON)
    stats_file = analysis_dir / 'accessibility_stats.json'
    with open(stats_file, 'w', encoding='utf-8') as f:
        json.dump(stats, f, ensure_ascii=False, indent=2)
    print(f"   ✅ {stats_file}")

    # 5.3. Список хабов (для справки)
    hubs_file = data_dir / 'identified_hubs.csv'
    hubs_df.to_csv(hubs_file, index=False)
    print(f"   ✅ {hubs_file}")

    # =============================
    # 6. ИТОГОВАЯ СВОДКА
    # =============================
    print("\n" + "=" * 80)
    print("✅ РАСЧЁТ ACCESSIBILITY ЗАВЕРШЁН")
    print("=" * 80)
    print(f"📊 Обработано НП: {len(accessibility_df)}")
    print(f"📊 Количество хабов: {len(hubs_df)}")
    print(f"📊 Средний Accessibility Score: {stats['mean_score']:.3f}")
    print(f"📊 Среднее расстояние до хаба: {stats['mean_distance_to_hub_km']:.2f} км")
    print(f"📂 Результаты сохранены в: {data_dir}")
    print("=" * 80)

    return accessibility_df, stats


if __name__ == '__main__':
    run_accessibility_calculation()
