#!/usr/bin/env python3
"""
Скрипт 05: Интерактивная карта кластеров

Цель: Визуализировать 128 НП на карте с цветовой кодировкой по кластерам
Библиотека: Plotly Scattergeo (или Folium как альтернатива)
Результат:
    - map_clusters.html (интерактивная карта)
"""

import pandas as pd
import numpy as np
from pathlib import Path
import warnings

warnings.filterwarnings('ignore')


def create_plotly_map(clusters_df):
    """Создаёт интерактивную карту через Plotly Scattergeo"""

    try:
        import plotly.express as px
        import plotly.graph_objects as go

        # Палитра цветов для кластеров (9 цветов)
        colors = [
            '#1f77b4',  # синий
            '#ff7f0e',  # оранжевый
            '#2ca02c',  # зелёный
            '#d62728',  # красный
            '#9467bd',  # фиолетовый
            '#8c564b',  # коричневый
            '#e377c2',  # розовый
            '#7f7f7f',  # серый
            '#bcbd22',  # жёлто-зелёный
        ]

        # Создаём цветовую карту cluster_id -> color
        cluster_ids = sorted(clusters_df['cluster_id'].unique())
        color_map = {cid: colors[i % len(colors)] for i, cid in enumerate(cluster_ids)}

        # Добавляем цвет к датафрейму
        clusters_df['color'] = clusters_df['cluster_id'].map(color_map)

        # Hover text
        clusters_df['hover_text'] = (
            '<b>' + clusters_df['settlement_name'] + '</b><br>' +
            'Регион: ' + clusters_df['region_name'] + '<br>' +
            'Население: ' + clusters_df['population'].apply(lambda x: f'{x:,.0f}') + ' чел.<br>' +
            'Кластер: ' + clusters_df['cluster_name'] + '<br>' +
            'Арктический: ' + clusters_df['is_arctic'].apply(lambda x: 'Да' if x else 'Нет')
        )

        # Создаём фигуру
        fig = go.Figure()

        # Добавляем точки для каждого кластера (для легенды)
        for cluster_id in cluster_ids:
            cluster_data = clusters_df[clusters_df['cluster_id'] == cluster_id]
            cluster_name = cluster_data['cluster_name'].iloc[0]

            fig.add_trace(go.Scattergeo(
                lon=cluster_data['longitude'],
                lat=cluster_data['latitude'],
                mode='markers',
                marker=dict(
                    size=cluster_data['population'] / 500,  # масштабируем размер
                    color=color_map[cluster_id],
                    line=dict(width=0.5, color='white'),
                    sizemode='area',
                    sizemin=4
                ),
                text=cluster_data['hover_text'],
                hoverinfo='text',
                name=f'{cluster_name} ({len(cluster_data)} НП)'
            ))

        # Настройки карты (фокус на Россию, северные регионы)
        fig.update_geos(
            scope='europe',
            showcountries=True,
            countrycolor="lightgray",
            showland=True,
            landcolor="white",
            showlakes=True,
            lakecolor="lightblue",
            projection_type="mercator",
            center=dict(lat=65, lon=90),  # Центр на северных регионах России
            projection_scale=2.5
        )

        # Общие настройки
        fig.update_layout(
            title={
                'text': 'Кластеры населённых пунктов Севера России (128 НП)',
                'x': 0.5,
                'xanchor': 'center',
                'font': {'size': 20}
            },
            showlegend=True,
            legend=dict(
                title=dict(text='Типы кластеров:', font=dict(size=14)),
                orientation="v",
                yanchor="top",
                y=0.99,
                xanchor="left",
                x=0.01,
                bgcolor="rgba(255, 255, 255, 0.8)"
            ),
            height=800,
            margin=dict(l=0, r=0, t=60, b=0)
        )

        return fig

    except ImportError:
        print("   ⚠️  Plotly не установлен. Попытка использовать Folium...")
        return None


def create_folium_map(clusters_df):
    """Альтернатива: создаёт карту через Folium"""

    try:
        import folium
        from folium import plugins

        # Центр карты (средние координаты)
        center_lat = clusters_df['latitude'].mean()
        center_lon = clusters_df['longitude'].mean()

        # Создаём базовую карту
        m = folium.Map(
            location=[center_lat, center_lon],
            zoom_start=4,
            tiles='OpenStreetMap'
        )

        # Палитра цветов
        colors_folium = [
            'blue', 'orange', 'green', 'red', 'purple',
            'darkred', 'pink', 'gray', 'lightgreen'
        ]

        cluster_ids = sorted(clusters_df['cluster_id'].unique())
        color_map = {cid: colors_folium[i % len(colors_folium)] for i, cid in enumerate(cluster_ids)}

        # Добавляем маркеры
        for _, row in clusters_df.iterrows():
            folium.CircleMarker(
                location=[row['latitude'], row['longitude']],
                radius=np.sqrt(row['population']) / 30,  # масштабируем размер
                color=color_map[row['cluster_id']],
                fill=True,
                fillColor=color_map[row['cluster_id']],
                fillOpacity=0.6,
                popup=folium.Popup(
                    f"<b>{row['settlement_name']}</b><br>"
                    f"Регион: {row['region_name']}<br>"
                    f"Население: {row['population']:,.0f} чел.<br>"
                    f"Кластер: {row['cluster_name']}<br>"
                    f"Арктический: {'Да' if row['is_arctic'] else 'Нет'}",
                    max_width=300
                ),
                tooltip=row['settlement_name']
            ).add_to(m)

        # Добавляем легенду (упрощённая)
        legend_html = '''
        <div style="position: fixed;
                    bottom: 50px; left: 50px; width: 300px; height: auto;
                    background-color: white; z-index:9999; font-size:14px;
                    border:2px solid grey; border-radius: 5px; padding: 10px">
        <p><b>Кластеры:</b></p>
        '''

        for cluster_id in cluster_ids:
            cluster_data = clusters_df[clusters_df['cluster_id'] == cluster_id]
            cluster_name = cluster_data['cluster_name'].iloc[0]
            count = len(cluster_data)
            color = color_map[cluster_id]

            legend_html += f'<p><span style="color:{color};">●</span> {cluster_name} ({count} НП)</p>'

        legend_html += '</div>'
        m.get_root().html.add_child(folium.Element(legend_html))

        # Добавляем fullscreen plugin
        plugins.Fullscreen().add_to(m)

        return m

    except ImportError:
        print("   ❌ Folium не установлен.")
        return None


def run_visualization():
    """Основная функция визуализации"""

    # Пути к файлам
    script_dir = Path(__file__).parent
    data_dir = script_dir.parent / 'results' / 'data'
    viz_dir = script_dir.parent / 'results' / 'visualization'
    viz_dir.mkdir(parents=True, exist_ok=True)

    # =============================
    # 1. ЗАГРУЗКА ДАННЫХ
    # =============================
    print("📂 Загрузка данных кластеров...")

    clusters_file = data_dir / 'clusters.csv'
    if not clusters_file.exists():
        print(f"   ❌ Файл не найден: {clusters_file}")
        print("   Сначала запустите 04_clustering.py")
        return

    clusters_df = pd.read_csv(clusters_file)
    print(f"   ✅ Загружено {len(clusters_df)} НП с кластерами")

    # Загружаем координаты (из settlements_with_indicators.csv)
    settlements_file = data_dir / 'settlements_with_indicators.csv'
    settlements_df = pd.read_csv(settlements_file)

    # Объединяем с координатами и арктичностью
    clusters_df = clusters_df.merge(
        settlements_df[['settlement_id', 'latitude', 'longitude', 'is_arctic']],
        on='settlement_id',
        how='left'
    )

    # Проверяем координаты
    missing_coords = clusters_df[clusters_df['latitude'].isna() | clusters_df['longitude'].isna()]
    if not missing_coords.empty:
        print(f"   ⚠️  У {len(missing_coords)} НП отсутствуют координаты:")
        print(missing_coords[['settlement_id', 'settlement_name']].to_string(index=False))
        # Убираем НП без координат
        clusters_df = clusters_df.dropna(subset=['latitude', 'longitude'])

    print(f"   ℹ️  НП с координатами: {len(clusters_df)}")
    print(f"   ℹ️  Уникальных кластеров: {clusters_df['cluster_id'].nunique()}")

    # =============================
    # 2. СОЗДАНИЕ КАРТЫ (Plotly)
    # =============================
    print("\n🗺️  Создание интерактивной карты (Plotly)...")

    fig = create_plotly_map(clusters_df)

    if fig is not None:
        # Сохраняем HTML
        output_file = viz_dir / 'map_clusters.html'
        fig.write_html(str(output_file))
        print(f"   ✅ Карта сохранена: {output_file}")
        print(f"   📊 Размер файла: {output_file.stat().st_size / 1024:.1f} KB")
    else:
        # Fallback на Folium
        print("\n🗺️  Создание карты через Folium (запасной вариант)...")
        m = create_folium_map(clusters_df)

        if m is not None:
            output_file = viz_dir / 'map_clusters.html'
            m.save(str(output_file))
            print(f"   ✅ Карта сохранена: {output_file}")
        else:
            print("   ❌ Не удалось создать карту. Установите plotly или folium:")
            print("      pip install plotly")
            print("      pip install folium")
            return

    # =============================
    # 3. СТАТИСТИКА ПО КАРТЕ
    # =============================
    print("\n📊 Статистика по карте:")
    print("=" * 60)
    print(f"✅ Всего НП на карте: {len(clusters_df)}")
    print(f"✅ Количество кластеров: {clusters_df['cluster_id'].nunique()}")
    print(f"✅ Арктических НП: {clusters_df['is_arctic'].sum()}")
    print(f"✅ Общее население: {clusters_df['population'].sum():,.0f} чел.")
    print("=" * 60)

    print("\n📍 Распределение по кластерам:")
    cluster_summary = clusters_df.groupby(['cluster_id', 'cluster_name']).agg({
        'settlement_id': 'count',
        'population': 'sum'
    }).rename(columns={'settlement_id': 'count', 'population': 'total_population'})

    for (cluster_id, cluster_name), row in cluster_summary.iterrows():
        print(f"   • {cluster_name}: {row['count']} НП, {row['total_population']:,.0f} чел.")

    print("\n" + "=" * 60)
    print("✅ ВИЗУАЛИЗАЦИЯ ЗАВЕРШЕНА")
    print("=" * 60)
    print(f"📂 Откройте карту в браузере: {output_file}")
    print("=" * 60)


if __name__ == '__main__':
    run_visualization()
