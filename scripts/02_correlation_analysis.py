#!/usr/bin/env python3
"""
Скрипт 02: Корреляционный анализ + PCA

Цель: Найти ключевые факторы из 93 показателей POAD
Результат:
    - correlation_matrix.csv (93×93)
    - top_10_factors.csv (топ-10 показателей по корреляции с населением)
    - pca_components.csv (главные компоненты)
    - pca_explained_variance.csv (доля объяснённой дисперсии)
"""

import pandas as pd
import numpy as np
from pathlib import Path
from sklearn.decomposition import PCA
from sklearn.preprocessing import StandardScaler
import warnings

warnings.filterwarnings('ignore')


def run_correlation_analysis():
    """Корреляционный анализ POAD показателей"""

    # Пути к файлам
    script_dir = Path(__file__).parent
    data_dir = script_dir.parent / 'results' / 'data'
    analysis_dir = script_dir.parent / 'results' / 'analysis'
    analysis_dir.mkdir(parents=True, exist_ok=True)

    # Загрузка данных
    print("📂 Загрузка данных из settlements_with_indicators.csv...")
    df = pd.read_csv(data_dir / 'settlements_with_indicators.csv')

    print(f"   ✅ Загружено {len(df)} НП")
    print(f"   ℹ️  Всего колонок: {len(df.columns)}")

    # Выбираем только числовые колонки (показатели POAD)
    # Исключаем метаданные (settlement_id, names, coordinates, etc.)
    meta_columns = [
        'settlement_id', 'settlement_name', 'region_name', 'municipality_up_name',
        'municipality_down_name', 'settlement_type', 'population', 'is_arctic',
        'latitude', 'longitude'
    ]

    # Оставляем только числовые колонки
    numeric_columns = df.select_dtypes(include=[np.number]).columns.tolist()
    indicator_columns = [col for col in numeric_columns if col not in meta_columns]

    print(f"   ℹ️  Показателей POAD: {len(indicator_columns)}")

    # Извлекаем матрицу показателей
    X = df[indicator_columns].copy()

    # Убираем колонки с пропусками > 50%
    missing_pct = (X.isna().sum() / len(X)) * 100
    columns_to_keep = missing_pct[missing_pct <= 50].index.tolist()

    print(f"\n🧹 Фильтрация показателей с пропусками > 50%...")
    print(f"   • Было: {len(indicator_columns)} показателей")
    print(f"   • Осталось: {len(columns_to_keep)} показателей")

    X_clean = X[columns_to_keep].copy()

    # Заполняем оставшиеся пропуски медианой
    X_clean = X_clean.fillna(X_clean.median())

    print(f"   ✅ Матрица данных: {X_clean.shape[0]} НП × {X_clean.shape[1]} показателей")

    # =============================
    # 1. КОРРЕЛЯЦИОННАЯ МАТРИЦА
    # =============================
    print("\n📊 Построение корреляционной матрицы...")
    corr_matrix = X_clean.corr()

    corr_file = analysis_dir / 'correlation_matrix.csv'
    corr_matrix.to_csv(corr_file)
    print(f"   ✅ {corr_file}")
    print(f"   ℹ️  Размерность: {corr_matrix.shape[0]}×{corr_matrix.shape[1]}")

    # =============================
    # 2. ТОП-10 ФАКТОРОВ
    # =============================
    print("\n🏆 Поиск топ-10 факторов по корреляции с населением...")

    # Если есть колонка population в исходном датафрейме
    if 'population' in df.columns:
        # Корреляция каждого показателя с населением
        population_corr = X_clean.corrwith(df['population']).abs().sort_values(ascending=False)

        # Топ-10 показателей
        top_10 = population_corr.head(10)

        print("\n   TOP-10 факторов (корреляция с населением):")
        for i, (indicator, corr_val) in enumerate(top_10.items(), 1):
            print(f"      {i}. {indicator}: r={corr_val:.3f}")

        # Сохраняем
        top_10_df = pd.DataFrame({
            'rank': range(1, 11),
            'indicator_code': top_10.index,
            'correlation_with_population': top_10.values
        })

        top_10_file = data_dir / 'top_10_factors.csv'
        top_10_df.to_csv(top_10_file, index=False)
        print(f"\n   ✅ {top_10_file}")

    else:
        print("   ⚠️  Колонка 'population' не найдена. Пропускаем корреляцию с населением.")
        top_10_df = None

    # =============================
    # 3. PCA (Снижение размерности)
    # =============================
    print("\n🔬 PCA: снижение размерности 93 → 15 компонент...")

    # Стандартизация данных
    scaler = StandardScaler()
    X_scaled = scaler.fit_transform(X_clean)

    # PCA с 15 компонентами
    n_components = min(15, X_clean.shape[1], X_clean.shape[0])
    pca = PCA(n_components=n_components, random_state=42)
    X_pca = pca.fit_transform(X_scaled)

    print(f"   ✅ PCA выполнен: {X_clean.shape[1]} → {n_components} компонент")

    # Доля объяснённой дисперсии
    explained_variance = pca.explained_variance_ratio_
    cumulative_variance = np.cumsum(explained_variance)

    print(f"\n   📈 Доля объяснённой дисперсии:")
    for i in range(min(10, n_components)):
        print(f"      PC{i+1}: {explained_variance[i]*100:.2f}% (累計: {cumulative_variance[i]*100:.2f}%)")

    # Сохраняем главные компоненты
    pca_df = pd.DataFrame(
        X_pca,
        columns=[f'PC{i+1}' for i in range(n_components)]
    )
    pca_df.insert(0, 'settlement_id', df['settlement_id'].values)

    pca_file = data_dir / 'pca_components.csv'
    pca_df.to_csv(pca_file, index=False)
    print(f"\n   ✅ {pca_file}")

    # Сохраняем explained variance
    variance_df = pd.DataFrame({
        'component': [f'PC{i+1}' for i in range(n_components)],
        'explained_variance_ratio': explained_variance,
        'cumulative_variance_ratio': cumulative_variance
    })

    variance_file = analysis_dir / 'pca_explained_variance.csv'
    variance_df.to_csv(variance_file, index=False)
    print(f"   ✅ {variance_file}")

    # =============================
    # 4. FEATURE IMPORTANCE (из PCA)
    # =============================
    print("\n🔍 Извлечение важности признаков из PCA...")

    # Loadings матрица (компоненты × признаки)
    loadings = pca.components_.T * np.sqrt(pca.explained_variance_)
    loadings_df = pd.DataFrame(
        loadings,
        columns=[f'PC{i+1}' for i in range(n_components)],
        index=X_clean.columns
    )

    # Считаем общую важность признака (сумма квадратов loadings по всем PC)
    feature_importance = (loadings_df ** 2).sum(axis=1).sort_values(ascending=False)

    print("\n   TOP-10 признаков по важности в PCA:")
    for i, (feature, importance) in enumerate(feature_importance.head(10).items(), 1):
        print(f"      {i}. {feature}: {importance:.3f}")

    # Сохраняем
    importance_df = pd.DataFrame({
        'indicator_code': feature_importance.index,
        'pca_importance': feature_importance.values
    })

    importance_file = data_dir / 'pca_feature_importance.csv'
    importance_df.to_csv(importance_file, index=False)
    print(f"\n   ✅ {importance_file}")

    # =============================
    # 5. СВОДКА
    # =============================
    print("\n" + "="*60)
    print("📊 РЕЗУЛЬТАТЫ АНАЛИЗА:")
    print("="*60)
    print(f"✅ Корреляционная матрица: {corr_matrix.shape[0]}×{corr_matrix.shape[1]}")
    if top_10_df is not None:
        print(f"✅ Топ-10 факторов по корреляции с населением")
    print(f"✅ PCA компоненты: {n_components} (объяснено {cumulative_variance[-1]*100:.2f}% дисперсии)")
    print(f"✅ Важность признаков в PCA сохранена")
    print("="*60)

    print(f"\n📂 Все результаты сохранены в:")
    print(f"   • {data_dir}")
    print(f"   • {analysis_dir}")


if __name__ == '__main__':
    run_correlation_analysis()
