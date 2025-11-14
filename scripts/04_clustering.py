#!/usr/bin/env python3
"""
Скрипт 04: K-means кластеризация населённых пунктов

Цель: Разбить 128 НП на типологические группы (5-7 кластеров)
Метод: K-means на PCA компонентах (15 штук)
Результат:
    - clusters.csv (settlement_id, cluster_id, cluster_name)
    - cluster_profiles.json (средние значения показателей по кластерам)
"""

import pandas as pd
import numpy as np
import json
from pathlib import Path
from sklearn.cluster import KMeans
from sklearn.metrics import silhouette_score, davies_bouldin_score
import warnings

warnings.filterwarnings('ignore')


def elbow_method(X, k_range=(3, 10)):
    """Метод локтя для определения оптимального k"""
    print("\n📉 Elbow Method: определение оптимального количества кластеров...")

    inertias = []
    silhouette_scores = []
    davies_bouldin_scores = []

    for k in range(k_range[0], k_range[1] + 1):
        kmeans = KMeans(n_clusters=k, random_state=42, n_init=20, max_iter=500)
        labels = kmeans.fit_predict(X)

        inertias.append(kmeans.inertia_)
        silhouette_scores.append(silhouette_score(X, labels))
        davies_bouldin_scores.append(davies_bouldin_score(X, labels))

        print(f"   k={k}: inertia={kmeans.inertia_:.2f}, "
              f"silhouette={silhouette_scores[-1]:.3f}, "
              f"davies_bouldin={davies_bouldin_scores[-1]:.3f}")

    # Рекомендация: выбираем k с максимальным silhouette score
    best_k_idx = np.argmax(silhouette_scores)
    best_k = range(k_range[0], k_range[1] + 1)[best_k_idx]

    print(f"\n   🏆 Рекомендуемое k = {best_k} (silhouette={silhouette_scores[best_k_idx]:.3f})")

    return {
        'k_values': list(range(k_range[0], k_range[1] + 1)),
        'inertias': inertias,
        'silhouette_scores': silhouette_scores,
        'davies_bouldin_scores': davies_bouldin_scores,
        'best_k': best_k
    }


def assign_cluster_names(cluster_stats_df, settlements_df):
    """Присваивает человекочитаемые имена кластерам на основе характеристик"""

    cluster_names = {}

    # Получаем ТОП-3 НП из каждого кластера для анализа
    for cluster_id in sorted(cluster_stats_df['cluster_id'].unique()):
        cluster_data = settlements_df[settlements_df['cluster_id'] == cluster_id]

        # Характеристики кластера
        avg_population = cluster_data['population'].mean() if 'population' in cluster_data.columns else 0
        count = len(cluster_data)
        arctic_ratio = cluster_data['is_arctic'].mean() if 'is_arctic' in cluster_data.columns else 0

        # Доминирующий регион
        top_region = cluster_data['region_name'].mode()[0] if 'region_name' in cluster_data.columns and not cluster_data['region_name'].empty else "Разные"

        # Определяем специализацию по региону
        is_hmao_yanao = cluster_data['region_name'].str.contains('Ханты-Мансийский|Ямало-Ненецкий', na=False).mean() > 0.5
        is_arctic = arctic_ratio > 0.5

        # Эвристика присвоения имён
        if count == 1:
            # Единичные кластеры - аномалии
            name = f"Аномалия: {cluster_data['settlement_name'].iloc[0]}"
        elif avg_population > 15000:
            if is_hmao_yanao:
                name = "Нефтегазовые посёлки (ХМАО/ЯНАО)"
            else:
                name = "Крупные ПГТ/города"
        elif avg_population > 7000:
            if is_arctic:
                name = "Арктические центры"
            else:
                name = "Средние города/ПГТ"
        elif avg_population > 4000:
            if is_arctic:
                name = "Арктические сёла"
            elif is_hmao_yanao:
                name = "Малые нефтегазовые ПГТ"
            else:
                name = "Стабильные сёла"
        elif avg_population > 2000:
            if is_arctic:
                name = "Малые арктические НП"
            else:
                name = "Сельские поселения"
        else:
            name = "Малые изолированные НП"

        # Проверяем дубли
        base_name = name
        counter = 1
        while name in cluster_names.values():
            name = f"{base_name} (вар.{counter})"
            counter += 1

        cluster_names[cluster_id] = name

    return cluster_names


def run_clustering():
    """Основная функция кластеризации"""

    # Пути к файлам
    script_dir = Path(__file__).parent
    data_dir = script_dir.parent / 'results' / 'data'
    analysis_dir = script_dir.parent / 'results' / 'analysis'

    # =============================
    # 1. ЗАГРУЗКА ДАННЫХ
    # =============================
    print("📂 Загрузка данных...")

    # PCA компоненты
    pca_df = pd.read_csv(data_dir / 'pca_components.csv')
    print(f"   ✅ PCA компоненты: {pca_df.shape[0]} НП × {pca_df.shape[1]-1} компонент")

    # Исходные данные (для метаданных и интерпретации)
    settlements_df = pd.read_csv(data_dir / 'settlements_with_indicators.csv')
    print(f"   ✅ Данные НП: {settlements_df.shape[0]} строк × {settlements_df.shape[1]} колонок")

    # Извлекаем только PCA компоненты (исключаем settlement_id)
    pca_columns = [col for col in pca_df.columns if col.startswith('PC')]
    X_pca = pca_df[pca_columns].values

    print(f"   ℹ️  Матрица для кластеризации: {X_pca.shape[0]} × {X_pca.shape[1]}")

    # =============================
    # 2. ELBOW METHOD
    # =============================
    elbow_results = elbow_method(X_pca, k_range=(3, 10))
    best_k = elbow_results['best_k']

    # =============================
    # 3. ФИНАЛЬНАЯ КЛАСТЕРИЗАЦИЯ
    # =============================
    print(f"\n🎯 Запуск K-means с k={best_k}...")

    kmeans = KMeans(n_clusters=best_k, random_state=42, n_init=20, max_iter=500)
    cluster_labels = kmeans.fit_predict(X_pca)

    # Метрики качества
    silhouette = silhouette_score(X_pca, cluster_labels)
    davies_bouldin = davies_bouldin_score(X_pca, cluster_labels)

    print(f"   ✅ Кластеризация завершена")
    print(f"   📊 Silhouette Score: {silhouette:.3f} (чем ближе к 1, тем лучше)")
    print(f"   📊 Davies-Bouldin Index: {davies_bouldin:.3f} (чем меньше, тем лучше)")

    # Добавляем метки кластеров к данным
    pca_df['cluster_id'] = cluster_labels
    settlements_df['cluster_id'] = cluster_labels

    # =============================
    # 4. АНАЛИЗ КЛАСТЕРОВ
    # =============================
    print("\n📊 Анализ кластеров:")
    print("=" * 80)

    cluster_stats = []

    for cluster_id in sorted(np.unique(cluster_labels)):
        cluster_mask = settlements_df['cluster_id'] == cluster_id
        cluster_data = settlements_df[cluster_mask]

        stats = {
            'cluster_id': int(cluster_id),
            'count': len(cluster_data),
            'avg_population': float(cluster_data['population'].mean()) if 'population' in cluster_data.columns else None,
            'total_population': int(cluster_data['population'].sum()) if 'population' in cluster_data.columns else None,
            'arctic_count': int(cluster_data['is_arctic'].sum()) if 'is_arctic' in cluster_data.columns else None,
        }

        cluster_stats.append(stats)

        print(f"\n🔵 Кластер {cluster_id}:")
        print(f"   • Кол-во НП: {stats['count']}")
        if stats['avg_population']:
            print(f"   • Среднее население: {stats['avg_population']:.0f}")
        if stats['total_population']:
            print(f"   • Общее население: {stats['total_population']:,}")
        if stats['arctic_count'] is not None:
            print(f"   • Арктических НП: {stats['arctic_count']}")

        # Примеры НП из кластера (топ-3 по населению)
        top_settlements = cluster_data.nlargest(3, 'population')[['settlement_name', 'population']] \
            if 'population' in cluster_data.columns else None

        if top_settlements is not None and not top_settlements.empty:
            print(f"   • Примеры НП:")
            for _, row in top_settlements.iterrows():
                print(f"      - {row['settlement_name']}: {row['population']:,.0f} чел.")

    print("\n" + "=" * 80)

    # =============================
    # 5. ПРИСВОЕНИЕ ИМЁН КЛАСТЕРАМ
    # =============================
    print("\n🏷️  Присвоение типологических имён кластерам...")

    cluster_stats_df = pd.DataFrame(cluster_stats)
    cluster_names = assign_cluster_names(cluster_stats_df, settlements_df)

    # Добавляем имена к датафреймам
    settlements_df['cluster_name'] = settlements_df['cluster_id'].map(cluster_names)

    print("\n   Имена кластеров:")
    for cluster_id, name in cluster_names.items():
        count = (settlements_df['cluster_id'] == cluster_id).sum()
        print(f"      Кластер {cluster_id}: '{name}' ({count} НП)")

    # =============================
    # 6. СОХРАНЕНИЕ РЕЗУЛЬТАТОВ
    # =============================
    print("\n💾 Сохранение результатов...")

    # 6.1. clusters.csv (компактная версия)
    clusters_output = settlements_df[[
        'settlement_id', 'settlement_name', 'region_name',
        'population', 'cluster_id', 'cluster_name'
    ]].copy()

    clusters_file = data_dir / 'clusters.csv'
    clusters_output.to_csv(clusters_file, index=False)
    print(f"   ✅ {clusters_file}")

    # 6.2. cluster_profiles.json
    profiles = []
    for cluster_id in sorted(cluster_names.keys()):
        cluster_data = settlements_df[settlements_df['cluster_id'] == cluster_id]

        # Вычисляем средние значения по всем числовым показателям
        numeric_cols = cluster_data.select_dtypes(include=[np.number]).columns
        means = cluster_data[numeric_cols].mean().to_dict()

        # Добавляем метаданные
        profile = {
            'cluster_id': int(cluster_id),
            'cluster_name': cluster_names[cluster_id],
            'count': len(cluster_data),
            'mean_values': {k: float(v) for k, v in means.items() if not pd.isna(v)}
        }

        profiles.append(profile)

    profiles_file = data_dir / 'cluster_profiles.json'
    with open(profiles_file, 'w', encoding='utf-8') as f:
        json.dump(profiles, f, ensure_ascii=False, indent=2)
    print(f"   ✅ {profiles_file}")

    # 6.3. Метрики кластеризации
    metrics = {
        'best_k': int(best_k),
        'silhouette_score': float(silhouette),
        'davies_bouldin_score': float(davies_bouldin),
        'elbow_method': elbow_results
    }

    metrics_file = analysis_dir / 'clustering_metrics.json'
    with open(metrics_file, 'w', encoding='utf-8') as f:
        json.dump(metrics, f, ensure_ascii=False, indent=2)
    print(f"   ✅ {metrics_file}")

    # =============================
    # 7. ИТОГОВАЯ СВОДКА
    # =============================
    print("\n" + "=" * 80)
    print("✅ КЛАСТЕРИЗАЦИЯ ЗАВЕРШЕНА")
    print("=" * 80)
    print(f"📊 Количество кластеров: {best_k}")
    print(f"📊 Silhouette Score: {silhouette:.3f}")
    print(f"📊 Davies-Bouldin Index: {davies_bouldin:.3f}")
    print(f"📂 Результаты сохранены в: {data_dir}")
    print("=" * 80)

    return {
        'clusters_df': settlements_df,
        'cluster_names': cluster_names,
        'metrics': metrics
    }


if __name__ == '__main__':
    run_clustering()
