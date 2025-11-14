#!/usr/bin/env python3
"""
Скрипт 03: Индекс привлекательности v1

Цель: Рассчитать базовый индекс привлекательности для 128 НП
Подход: Weighted sum топ-10 факторов из корреляционного анализа

Результат: attractiveness_v1.csv (128 НП с индексом 0-10)
"""

import pandas as pd
import numpy as np
from pathlib import Path
from sklearn.preprocessing import MinMaxScaler


def calculate_attractiveness_v1():
    """Расчёт индекса привлекательности v1"""

    # Пути к файлам
    script_dir = Path(__file__).parent
    data_dir = script_dir.parent / 'results' / 'data'

    print("📂 Загрузка данных...")

    # Загрузка базовых данных
    df_settlements = pd.read_csv(data_dir / 'settlements_with_indicators.csv')
    print(f"   ✅ settlements_with_indicators.csv: {len(df_settlements)} НП")

    # Загрузка топ-10 факторов
    top_10_file = data_dir / 'top_10_factors.csv'

    if top_10_file.exists():
        df_top_10 = pd.read_csv(top_10_file)
        # Конвертируем в строки (pandas может прочитать как float)
        top_indicators = df_top_10['indicator_code'].astype(str).tolist()
        print(f"   ✅ top_10_factors.csv: {len(top_indicators)} показателей")
    else:
        print("   ⚠️  top_10_factors.csv не найден. Используем PCA важность.")
        df_importance = pd.read_csv(data_dir / 'pca_feature_importance.csv')
        top_indicators = df_importance.head(10)['indicator_code'].astype(str).tolist()
        print(f"   ✅ Топ-10 из PCA важности")

    print(f"\n🎯 Используемые показатели для индекса:")
    for i, indicator in enumerate(top_indicators, 1):
        print(f"   {i}. {indicator}")

    # =============================
    # 1. ИЗВЛЕЧЕНИЕ ПОКАЗАТЕЛЕЙ
    # =============================
    print("\n📊 Извлечение данных по топ-10 показателям...")

    # Проверяем, какие показатели есть в данных
    available_indicators = [col for col in top_indicators if col in df_settlements.columns]

    if len(available_indicators) == 0:
        print("   ❌ ОШИБКА: Ни один из топ-10 показателей не найден в данных!")
        print("   Доступные колонки:", df_settlements.columns.tolist())
        return

    print(f"   ✅ Найдено {len(available_indicators)} из {len(top_indicators)} показателей")

    # Извлекаем данные
    X = df_settlements[available_indicators].copy()

    # Заполняем пропуски медианой
    X_filled = X.fillna(X.median())

    print(f"   ℹ️  Матрица: {X_filled.shape[0]} НП × {X_filled.shape[1]} показателей")

    # =============================
    # 2. НОРМАЛИЗАЦИЯ (0-1)
    # =============================
    print("\n🔧 Нормализация показателей (0-1 шкала)...")

    scaler = MinMaxScaler()
    X_normalized = scaler.fit_transform(X_filled)

    X_norm_df = pd.DataFrame(
        X_normalized,
        columns=available_indicators,
        index=df_settlements.index
    )

    print(f"   ✅ Нормализация завершена")

    # =============================
    # 3. РАСЧЁТ ИНДЕКСА
    # =============================
    print("\n📐 Расчёт индекса привлекательности v1...")

    # Веса: равные для всех показателей (можно настроить позже)
    weights = np.ones(len(available_indicators)) / len(available_indicators)

    print(f"   ℹ️  Веса: равномерные ({weights[0]:.3f} для каждого показателя)")

    # Weighted sum
    attractiveness_score = (X_norm_df * weights).sum(axis=1)

    # Масштабируем в 0-10
    attractiveness_score_scaled = attractiveness_score * 10

    print(f"   ✅ Индекс рассчитан для {len(attractiveness_score_scaled)} НП")

    # =============================
    # 4. СОЗДАНИЕ ИТОГОВОЙ ТАБЛИЦЫ
    # =============================
    print("\n📋 Создание итоговой таблицы...")

    df_result = df_settlements[[
        'settlement_id', 'settlement_name', 'region_name',
        'settlement_type', 'population', 'is_arctic', 'latitude', 'longitude'
    ]].copy()

    df_result['attractiveness_v1'] = attractiveness_score_scaled.values

    # Ранжирование
    df_result['attractiveness_rank'] = df_result['attractiveness_v1'].rank(
        ascending=False, method='min'
    ).astype(int)

    # Сортировка по индексу
    df_result = df_result.sort_values('attractiveness_v1', ascending=False).reset_index(drop=True)

    print(f"   ✅ Итоговая таблица: {len(df_result)} НП")

    # =============================
    # 5. СТАТИСТИКА
    # =============================
    print("\n" + "="*60)
    print("📊 СТАТИСТИКА ИНДЕКСА ПРИВЛЕКАТЕЛЬНОСТИ V1:")
    print("="*60)

    print(f"Среднее значение: {df_result['attractiveness_v1'].mean():.2f}")
    print(f"Медиана: {df_result['attractiveness_v1'].median():.2f}")
    print(f"Мин: {df_result['attractiveness_v1'].min():.2f}")
    print(f"Макс: {df_result['attractiveness_v1'].max():.2f}")
    print(f"Стд. отклонение: {df_result['attractiveness_v1'].std():.2f}")

    print("\n🏆 ТОП-10 ПРИВЛЕКАТЕЛЬНЫХ НП:")
    for idx, row in df_result.head(10).iterrows():
        print(f"   {row['attractiveness_rank']:>3}. {row['settlement_name']:<30} "
              f"({row['region_name']:<20}) | "
              f"Индекс: {row['attractiveness_v1']:.2f} | "
              f"Население: {row['population']:>6,.0f}")

    print("\n⚠️  ТОП-10 НАИМЕНЕЕ ПРИВЛЕКАТЕЛЬНЫХ НП:")
    for idx, row in df_result.tail(10).iterrows():
        print(f"   {row['attractiveness_rank']:>3}. {row['settlement_name']:<30} "
              f"({row['region_name']:<20}) | "
              f"Индекс: {row['attractiveness_v1']:.2f} | "
              f"Население: {row['population']:>6,.0f}")

    # Статистика по регионам
    print("\n📍 СРЕДНИЙ ИНДЕКС ПО РЕГИОНАМ:")
    region_stats = df_result.groupby('region_name')['attractiveness_v1'].agg(['mean', 'count']).sort_values('mean', ascending=False)
    for region, row in region_stats.iterrows():
        print(f"   {region:<30}: {row['mean']:.2f} (НП: {int(row['count'])})")

    # Арктические vs неарктические
    print("\n🌐 АРКТИЧЕСКИЕ VS НЕАРКТИЧЕСКИЕ:")
    arctic_mean = df_result[df_result['is_arctic']]['attractiveness_v1'].mean()
    non_arctic_mean = df_result[~df_result['is_arctic']]['attractiveness_v1'].mean()
    print(f"   Арктические НП: {arctic_mean:.2f} (n={df_result['is_arctic'].sum()})")
    print(f"   Неарктические НП: {non_arctic_mean:.2f} (n={(~df_result['is_arctic']).sum()})")

    print("="*60)

    # =============================
    # 6. СОХРАНЕНИЕ
    # =============================
    print("\n💾 Сохранение результатов...")

    output_file = data_dir / 'attractiveness_v1.csv'
    df_result.to_csv(output_file, index=False, encoding='utf-8')
    print(f"   ✅ {output_file}")

    print("\n✅ ИНДЕКС ПРИВЛЕКАТЕЛЬНОСТИ V1 ГОТОВ!")


if __name__ == '__main__':
    calculate_attractiveness_v1()
