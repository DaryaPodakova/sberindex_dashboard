#!/usr/bin/env python3
"""
Скрипт 09: Генерация интерактивного HTML дашборда

Цель: Создать статичный HTML дашборд с Plotly для визуализации всех результатов
Компоненты:
    1. Интерактивная карта кластеров
    2. ТОП-20 НП по индексу v2 (таблица)
    3. Графики сравнения v1 vs v2
    4. Статистика по кластерам
Результат:
    - dashboard.html — полностью автономный HTML файл
"""

import pandas as pd
import numpy as np
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import plotly.express as px
from pathlib import Path
import json
import warnings

warnings.filterwarnings('ignore')


def create_map_section(df):
    """Создаёт интерактивную карту кластеров"""

    # Цветовая палитра для кластеров
    colors = px.colors.qualitative.Plotly

    cluster_ids = sorted(df['cluster_id'].unique())
    color_map = {cid: colors[i % len(colors)] for i, cid in enumerate(cluster_ids)}

    # Создаём карту
    fig = go.Figure()

    for cluster_id in cluster_ids:
        cluster_data = df[df['cluster_id'] == cluster_id]
        cluster_name = cluster_data['cluster_name'].iloc[0]

        # Hover text
        hover_text = []
        for _, row in cluster_data.iterrows():
            text = (
                f"<b>{row['settlement_name']}</b><br>"
                f"Регион: {row['region_name']}<br>"
                f"Население: {row['population']:,.0f} чел.<br>"
                f"Кластер: {row['cluster_name']}<br>"
                f"Индекс v2: {row['attractiveness_v2_score_0_10']:.2f}/10<br>"
                f"Доступность: {row['accessibility_normalized']:.2f}<br>"
                f"Арктический: {'Да' if row['is_arctic'] else 'Нет'}"
            )
            hover_text.append(text)

        fig.add_trace(go.Scattergeo(
            lon=cluster_data['longitude'],
            lat=cluster_data['latitude'],
            mode='markers',
            marker=dict(
                size=cluster_data['population'] / 400,
                color=color_map[cluster_id],
                line=dict(width=0.5, color='white'),
                sizemode='area',
                sizemin=4
            ),
            text=hover_text,
            hoverinfo='text',
            name=f'{cluster_name} ({len(cluster_data)})'
        ))

    fig.update_geos(
        scope='europe',
        showcountries=True,
        countrycolor="lightgray",
        showland=True,
        landcolor="white",
        showlakes=True,
        lakecolor="lightblue",
        projection_type="mercator",
        center=dict(lat=65, lon=90),
        projection_scale=2.5
    )

    fig.update_layout(
        title={
            'text': '🗺️ Кластеры населённых пунктов Севера России (128 НП)',
            'x': 0.5,
            'xanchor': 'center',
            'font': {'size': 18}
        },
        showlegend=True,
        legend=dict(
            title=dict(text='<b>Типы кластеров:</b>', font=dict(size=12)),
            orientation="v",
            yanchor="top",
            y=0.99,
            xanchor="left",
            x=0.01,
            bgcolor="rgba(255, 255, 255, 0.9)"
        ),
        height=600,
        margin=dict(l=0, r=0, t=60, b=0)
    )

    return fig


def create_top_table(df, top_n=20):
    """Создаёт таблицу ТОП-N НП"""

    top_df = df.nlargest(top_n, 'attractiveness_v2_score_0_10')

    # Форматируем данные для таблицы
    table_data = []
    for i, (_, row) in enumerate(top_df.iterrows(), 1):
        table_data.append({
            'Ранг': i,
            'НП': row['settlement_name'],
            'Регион': row['region_name'][:30] + '...' if len(row['region_name']) > 30 else row['region_name'],
            'Население': f"{row['population']:,.0f}",
            'Кластер': row['cluster_name'],
            'Индекс v2': f"{row['attractiveness_v2_score_0_10']:.2f}",
            'POAD': f"{row['poad_normalized']:.2f}",
            'Доступность': f"{row['accessibility_normalized']:.2f}",
            'Арктика': 'Да' if row['is_arctic'] else 'Нет'
        })

    table_df = pd.DataFrame(table_data)

    # Создаём таблицу Plotly
    fig = go.Figure(data=[go.Table(
        header=dict(
            values=[f'<b>{col}</b>' for col in table_df.columns],
            fill_color='#2c3e50',
            font=dict(color='white', size=12),
            align='left',
            height=30
        ),
        cells=dict(
            values=[table_df[col] for col in table_df.columns],
            fill_color=[['#ecf0f1' if i % 2 == 0 else 'white' for i in range(len(table_df))]],
            align='left',
            font=dict(size=11),
            height=25
        )
    )])

    fig.update_layout(
        title={
            'text': f'🏆 ТОП-{top_n} НП по индексу привлекательности v2',
            'x': 0.5,
            'xanchor': 'center',
            'font': {'size': 16}
        },
        height=600,
        margin=dict(l=20, r=20, t=60, b=20)
    )

    return fig


def create_comparison_charts(df):
    """Создаёт графики сравнения v1 vs v2"""

    # Subplot с 2 графиками
    fig = make_subplots(
        rows=1, cols=2,
        subplot_titles=(
            '📈 Наибольшее улучшение (v2 > v1)',
            '📉 Наибольшее ухудшение (v2 < v1)'
        ),
        horizontal_spacing=0.15
    )

    # График улучшений
    top_improved = df.nlargest(10, 'delta_v2_minus_v1')
    fig.add_trace(
        go.Bar(
            y=top_improved['settlement_name'],
            x=top_improved['delta_v2_minus_v1'],
            orientation='h',
            marker=dict(color='#27ae60'),
            text=top_improved['delta_v2_minus_v1'].apply(lambda x: f'+{x:.3f}'),
            textposition='outside',
            hovertemplate='<b>%{y}</b><br>Δ: %{x:.3f}<extra></extra>'
        ),
        row=1, col=1
    )

    # График ухудшений
    top_worsened = df.nsmallest(10, 'delta_v2_minus_v1')
    fig.add_trace(
        go.Bar(
            y=top_worsened['settlement_name'],
            x=top_worsened['delta_v2_minus_v1'],
            orientation='h',
            marker=dict(color='#e74c3c'),
            text=top_worsened['delta_v2_minus_v1'].apply(lambda x: f'{x:.3f}'),
            textposition='outside',
            hovertemplate='<b>%{y}</b><br>Δ: %{x:.3f}<extra></extra>'
        ),
        row=1, col=2
    )

    fig.update_xaxes(title_text='Изменение индекса (v2 - v1)', row=1, col=1)
    fig.update_xaxes(title_text='Изменение индекса (v2 - v1)', row=1, col=2)

    fig.update_layout(
        title={
            'text': '🔄 Сравнение индекса v1 vs v2 (влияние транспортной доступности)',
            'x': 0.5,
            'xanchor': 'center',
            'font': {'size': 16}
        },
        showlegend=False,
        height=500,
        margin=dict(l=20, r=20, t=80, b=40)
    )

    return fig


def create_cluster_stats(df):
    """Создаёт статистику по кластерам"""

    # Группируем по кластерам
    cluster_stats = df.groupby(['cluster_id', 'cluster_name']).agg({
        'settlement_id': 'count',
        'population': 'sum',
        'attractiveness_v2_score_0_10': 'mean',
        'accessibility_normalized': 'mean',
        'is_arctic': 'sum'
    }).reset_index()

    cluster_stats.columns = ['cluster_id', 'cluster_name', 'count', 'total_population',
                              'avg_v2', 'avg_accessibility', 'arctic_count']

    cluster_stats = cluster_stats.sort_values('avg_v2', ascending=True)

    # Subplot с 2 графиками
    fig = make_subplots(
        rows=1, cols=2,
        subplot_titles=(
            'Средний индекс v2 по кластерам',
            'Население по кластерам'
        ),
        specs=[[{'type': 'bar'}, {'type': 'bar'}]],
        horizontal_spacing=0.15
    )

    # График среднего индекса
    fig.add_trace(
        go.Bar(
            y=cluster_stats['cluster_name'],
            x=cluster_stats['avg_v2'],
            orientation='h',
            marker=dict(
                color=cluster_stats['avg_v2'],
                colorscale='RdYlGn',
                showscale=False
            ),
            text=cluster_stats['avg_v2'].apply(lambda x: f'{x:.2f}'),
            textposition='outside',
            hovertemplate='<b>%{y}</b><br>Средний v2: %{x:.2f}/10<extra></extra>'
        ),
        row=1, col=1
    )

    # График населения
    fig.add_trace(
        go.Bar(
            y=cluster_stats['cluster_name'],
            x=cluster_stats['total_population'],
            orientation='h',
            marker=dict(color='#3498db'),
            text=cluster_stats['total_population'].apply(lambda x: f'{x:,.0f}'),
            textposition='outside',
            hovertemplate='<b>%{y}</b><br>Население: %{x:,.0f} чел.<extra></extra>'
        ),
        row=1, col=2
    )

    fig.update_xaxes(title_text='Средний индекс v2 (0-10)', row=1, col=1)
    fig.update_xaxes(title_text='Общее население', row=1, col=2)

    fig.update_layout(
        title={
            'text': '📊 Статистика по кластерам',
            'x': 0.5,
            'xanchor': 'center',
            'font': {'size': 16}
        },
        showlegend=False,
        height=500,
        margin=dict(l=20, r=20, t=80, b=40)
    )

    return fig


def build_dashboard():
    """Основная функция создания дашборда"""

    # Пути к файлам
    script_dir = Path(__file__).parent
    data_dir = script_dir.parent / 'results' / 'data'
    viz_dir = script_dir.parent / 'results' / 'visualization'
    viz_dir.mkdir(parents=True, exist_ok=True)

    print("📂 Загрузка данных для дашборда...")

    # Загружаем данные
    attractiveness_v2_df = pd.read_csv(data_dir / 'attractiveness_v2.csv')
    comparison_df = pd.read_csv(data_dir / 'comparison_v1_vs_v2.csv')
    clusters_df = pd.read_csv(data_dir / 'clusters.csv')
    settlements_df = pd.read_csv(data_dir / 'settlements_with_indicators.csv')

    # Объединяем данные
    df = attractiveness_v2_df.merge(
        comparison_df[['settlement_id', 'delta_v2_minus_v1', 'rank_v1', 'rank_v2', 'rank_change']],
        on='settlement_id',
        how='left'
    )

    # Добавляем cluster_id и координаты
    if 'cluster_id' not in df.columns:
        df = df.merge(
            clusters_df[['settlement_id', 'cluster_id']],
            on='settlement_id',
            how='left'
        )

    if 'latitude' not in df.columns or 'longitude' not in df.columns:
        df = df.merge(
            settlements_df[['settlement_id', 'latitude', 'longitude']],
            on='settlement_id',
            how='left'
        )

    print(f"   ✅ Загружено {len(df)} НП")

    # Создаём секции дашборда
    print("\n🎨 Создание компонентов дашборда...")

    print("   1/4 Интерактивная карта кластеров...")
    map_fig = create_map_section(df)

    print("   2/4 Таблица ТОП-20...")
    table_fig = create_top_table(df, top_n=20)

    print("   3/4 Графики сравнения v1 vs v2...")
    comparison_fig = create_comparison_charts(df)

    print("   4/4 Статистика по кластерам...")
    cluster_fig = create_cluster_stats(df)

    # Собираем HTML
    print("\n📦 Сборка HTML дашборда...")

    html_parts = []

    # Header
    html_parts.append("""
<!DOCTYPE html>
<html lang="ru">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>Arctic Viability Index - Dashboard</title>
    <style>
        body {
            font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, Oxygen, Ubuntu, Cantarell, sans-serif;
            margin: 0;
            padding: 0;
            background: #f5f7fa;
        }
        .header {
            background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
            color: white;
            padding: 30px 20px;
            text-align: center;
            box-shadow: 0 2px 10px rgba(0,0,0,0.1);
        }
        .header h1 {
            margin: 0;
            font-size: 2.5em;
            font-weight: 700;
        }
        .header p {
            margin: 10px 0 0 0;
            font-size: 1.1em;
            opacity: 0.95;
        }
        .container {
            max-width: 1400px;
            margin: 30px auto;
            padding: 0 20px;
        }
        .section {
            background: white;
            border-radius: 10px;
            padding: 20px;
            margin-bottom: 30px;
            box-shadow: 0 2px 8px rgba(0,0,0,0.08);
        }
        .stats-grid {
            display: grid;
            grid-template-columns: repeat(auto-fit, minmax(200px, 1fr));
            gap: 20px;
            margin: 20px 0;
        }
        .stat-card {
            background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
            color: white;
            padding: 20px;
            border-radius: 8px;
            text-align: center;
        }
        .stat-card h3 {
            margin: 0 0 10px 0;
            font-size: 2em;
            font-weight: 700;
        }
        .stat-card p {
            margin: 0;
            opacity: 0.9;
            font-size: 0.95em;
        }
        .footer {
            text-align: center;
            padding: 30px;
            color: #666;
            font-size: 0.9em;
        }
    </style>
</head>
<body>
    <div class="header">
        <h1>🏔️ Arctic Viability Index</h1>
        <p>Индекс привлекательности для 128 населённых пунктов северных регионов России</p>
    </div>

    <div class="container">
        <div class="stats-grid">
            <div class="stat-card">
                <h3>128</h3>
                <p>Населённых пунктов</p>
            </div>
            <div class="stat-card">
                <h3>653,347</h3>
                <p>Общее население</p>
            </div>
            <div class="stat-card">
                <h3>9</h3>
                <p>Кластеров НП</p>
            </div>
            <div class="stat-card">
                <h3>93</h3>
                <p>Показателя POAD</p>
            </div>
        </div>
""")

    # Добавляем графики
    html_parts.append('<div class="section">')
    html_parts.append(map_fig.to_html(full_html=False, include_plotlyjs='cdn'))
    html_parts.append('</div>')

    html_parts.append('<div class="section">')
    html_parts.append(table_fig.to_html(full_html=False, include_plotlyjs=False))
    html_parts.append('</div>')

    html_parts.append('<div class="section">')
    html_parts.append(comparison_fig.to_html(full_html=False, include_plotlyjs=False))
    html_parts.append('</div>')

    html_parts.append('<div class="section">')
    html_parts.append(cluster_fig.to_html(full_html=False, include_plotlyjs=False))
    html_parts.append('</div>')

    # Footer
    html_parts.append("""
    </div>

    <div class="footer">
        <p><strong>Arctic Viability Index Dashboard</strong></p>
        <p>Создано с использованием: Python, Pandas, Plotly, scikit-learn</p>
        <p>Дата создания: 2025-11-04 | Claude Code + Sberbank Hackathon 2025</p>
    </div>
</body>
</html>
""")

    # Сохраняем HTML
    dashboard_file = viz_dir / 'dashboard.html'
    with open(dashboard_file, 'w', encoding='utf-8') as f:
        f.write(''.join(html_parts))

    print(f"\n✅ Дашборд создан: {dashboard_file}")
    print(f"📊 Размер файла: {dashboard_file.stat().st_size / 1024:.1f} KB")

    print("\n" + "=" * 80)
    print("✅ ДАШБОРД ГОТОВ!")
    print("=" * 80)
    print(f"📂 Откройте файл в браузере: {dashboard_file}")
    print("   Или используйте: file://" + str(dashboard_file.absolute()))
    print("=" * 80)

    return dashboard_file


if __name__ == '__main__':
    build_dashboard()
