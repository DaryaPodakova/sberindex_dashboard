#!/usr/bin/env python3
"""
Скрипт 06: Матрица расстояний между населёнными пунктами

Цель: Рассчитать географические расстояния между всеми 128 НП
Метод: Haversine formula (расстояния по большому кругу на сфере)
Результат:
    - distance_matrix.npy — матрица 128×128 в километрах
    - distance_stats.json — статистика (min/max/mean расстояния)
"""

import pandas as pd
import numpy as np
import json
from pathlib import Path
import warnings

warnings.filterwarnings('ignore')


def haversine_distance(lat1, lon1, lat2, lon2):
    """
    Вычисляет расстояние между двумя точками на Земле по формуле Haversine

    Args:
        lat1, lon1: координаты первой точки (градусы)
        lat2, lon2: координаты второй точки (градусы)

    Returns:
        расстояние в километрах
    """
    # Радиус Земли в километрах
    R = 6371.0

    # Конвертация в радианы
    lat1_rad = np.radians(lat1)
    lon1_rad = np.radians(lon1)
    lat2_rad = np.radians(lat2)
    lon2_rad = np.radians(lon2)

    # Разница координат
    dlat = lat2_rad - lat1_rad
    dlon = lon2_rad - lon1_rad

    # Формула Haversine
    a = np.sin(dlat / 2)**2 + np.cos(lat1_rad) * np.cos(lat2_rad) * np.sin(dlon / 2)**2
    c = 2 * np.arctan2(np.sqrt(a), np.sqrt(1 - a))

    distance = R * c
    return distance


def compute_distance_matrix(settlements_df):
    """
    Вычисляет матрицу расстояний между всеми НП

    Args:
        settlements_df: DataFrame с колонками settlement_id, latitude, longitude

    Returns:
        numpy array размера n×n с расстояниями в км
    """
    n = len(settlements_df)
    distance_matrix = np.zeros((n, n))

    print(f"\n📏 Вычисление матрицы расстояний {n}×{n}...")

    # Извлекаем координаты
    coords = settlements_df[['latitude', 'longitude']].values

    # Вычисляем расстояния между всеми парами
    for i in range(n):
        for j in range(i + 1, n):
            lat1, lon1 = coords[i]
            lat2, lon2 = coords[j]

            dist = haversine_distance(lat1, lon1, lat2, lon2)
            distance_matrix[i, j] = dist
            distance_matrix[j, i] = dist  # симметричная матрица

        if (i + 1) % 20 == 0:
            print(f"   Обработано {i + 1}/{n} НП...")

    print(f"   ✅ Матрица расстояний вычислена")

    return distance_matrix


def analyze_distances(distance_matrix, settlements_df):
    """Анализ статистики расстояний"""

    print("\n📊 Статистика расстояний:")
    print("=" * 60)

    # Убираем нули (расстояния НП до самого себя)
    non_zero_distances = distance_matrix[distance_matrix > 0]

    stats = {
        'min_distance_km': float(non_zero_distances.min()),
        'max_distance_km': float(non_zero_distances.max()),
        'mean_distance_km': float(non_zero_distances.mean()),
        'median_distance_km': float(np.median(non_zero_distances)),
        'std_distance_km': float(non_zero_distances.std())
    }

    print(f"   • Минимальное расстояние: {stats['min_distance_km']:.2f} км")
    print(f"   • Максимальное расстояние: {stats['max_distance_km']:.2f} км")
    print(f"   • Среднее расстояние: {stats['mean_distance_km']:.2f} км")
    print(f"   • Медианное расстояние: {stats['median_distance_km']:.2f} км")
    print(f"   • Стандартное отклонение: {stats['std_distance_km']:.2f} км")

    # Найдём самую близкую пару НП
    min_dist_idx = np.unravel_index(
        np.argmin(distance_matrix + np.eye(len(distance_matrix)) * 1e9),  # игнорируем диагональ
        distance_matrix.shape
    )

    closest_pair = {
        'settlement_1': settlements_df.iloc[min_dist_idx[0]]['settlement_name'],
        'settlement_2': settlements_df.iloc[min_dist_idx[1]]['settlement_name'],
        'distance_km': float(distance_matrix[min_dist_idx])
    }

    print(f"\n   🏆 Самая близкая пара НП:")
    print(f"      {closest_pair['settlement_1']} ↔ {closest_pair['settlement_2']}")
    print(f"      Расстояние: {closest_pair['distance_km']:.2f} км")

    # Найдём самую дальнюю пару НП
    max_dist_idx = np.unravel_index(np.argmax(distance_matrix), distance_matrix.shape)

    furthest_pair = {
        'settlement_1': settlements_df.iloc[max_dist_idx[0]]['settlement_name'],
        'settlement_2': settlements_df.iloc[max_dist_idx[1]]['settlement_name'],
        'distance_km': float(distance_matrix[max_dist_idx])
    }

    print(f"\n   🌍 Самая дальняя пара НП:")
    print(f"      {furthest_pair['settlement_1']} ↔ {furthest_pair['settlement_2']}")
    print(f"      Расстояние: {furthest_pair['distance_km']:.2f} км")

    print("=" * 60)

    stats['closest_pair'] = closest_pair
    stats['furthest_pair'] = furthest_pair

    return stats


def run_distance_calculation():
    """Основная функция расчёта расстояний"""

    # Пути к файлам
    script_dir = Path(__file__).parent
    data_dir = script_dir.parent / 'results' / 'data'
    analysis_dir = script_dir.parent / 'results' / 'analysis'

    # =============================
    # 1. ЗАГРУЗКА ДАННЫХ
    # =============================
    print("📂 Загрузка данных населённых пунктов...")

    settlements_file = data_dir / 'settlements_with_indicators.csv'
    if not settlements_file.exists():
        print(f"   ❌ Файл не найден: {settlements_file}")
        return

    settlements_df = pd.read_csv(settlements_file)
    print(f"   ✅ Загружено {len(settlements_df)} НП")

    # Проверяем координаты
    missing_coords = settlements_df[
        settlements_df['latitude'].isna() | settlements_df['longitude'].isna()
    ]

    if not missing_coords.empty:
        print(f"   ⚠️  У {len(missing_coords)} НП отсутствуют координаты:")
        print(missing_coords[['settlement_id', 'settlement_name']].to_string(index=False))
        # Удаляем НП без координат
        settlements_df = settlements_df.dropna(subset=['latitude', 'longitude'])
        print(f"   ℹ️  После фильтрации: {len(settlements_df)} НП")

    # =============================
    # 2. ВЫЧИСЛЕНИЕ МАТРИЦЫ РАССТОЯНИЙ
    # =============================
    distance_matrix = compute_distance_matrix(settlements_df)

    # =============================
    # 3. АНАЛИЗ СТАТИСТИКИ
    # =============================
    stats = analyze_distances(distance_matrix, settlements_df)

    # =============================
    # 4. СОХРАНЕНИЕ РЕЗУЛЬТАТОВ
    # =============================
    print("\n💾 Сохранение результатов...")

    # 4.1. Матрица расстояний (numpy binary format)
    distance_matrix_file = data_dir / 'distance_matrix.npy'
    np.save(distance_matrix_file, distance_matrix)
    print(f"   ✅ {distance_matrix_file}")
    print(f"   📊 Размер: {distance_matrix_file.stat().st_size / 1024:.1f} KB")
    print(f"   ℹ️  Форма матрицы: {distance_matrix.shape}")

    # 4.2. Статистика (JSON)
    stats_file = analysis_dir / 'distance_stats.json'
    with open(stats_file, 'w', encoding='utf-8') as f:
        json.dump(stats, f, ensure_ascii=False, indent=2)
    print(f"   ✅ {stats_file}")

    # 4.3. Метаданные (порядок НП в матрице)
    metadata = {
        'settlement_ids': settlements_df['settlement_id'].tolist(),
        'settlement_names': settlements_df['settlement_name'].tolist(),
        'matrix_shape': list(distance_matrix.shape),
        'description': 'Матрица расстояний между НП (км). Индексы соответствуют порядку в settlement_ids.'
    }

    metadata_file = data_dir / 'distance_matrix_metadata.json'
    with open(metadata_file, 'w', encoding='utf-8') as f:
        json.dump(metadata, f, ensure_ascii=False, indent=2)
    print(f"   ✅ {metadata_file}")

    # =============================
    # 5. ИТОГОВАЯ СВОДКА
    # =============================
    print("\n" + "=" * 60)
    print("✅ РАСЧЁТ РАССТОЯНИЙ ЗАВЕРШЁН")
    print("=" * 60)
    print(f"📊 Матрица расстояний: {distance_matrix.shape[0]}×{distance_matrix.shape[1]}")
    print(f"📊 Среднее расстояние: {stats['mean_distance_km']:.2f} км")
    print(f"📊 Диапазон: {stats['min_distance_km']:.2f} - {stats['max_distance_km']:.2f} км")
    print(f"📂 Результаты сохранены в: {data_dir}")
    print("=" * 60)

    return distance_matrix, stats


if __name__ == '__main__':
    run_distance_calculation()
