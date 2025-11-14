#!/usr/bin/env python3
"""
Скрипт 10: Дашборд с интерактивными фильтрами (улучшенная версия)

Цель: Создать дашборд с кнопками фильтрации (как в Power BI)
Фильтры:
    - Арктика / Не-Арктика / Все
    - По кластерам
Результат:
    - dashboard_filtered.html — дашборд с интерактивными кнопками
"""

import pandas as pd
import numpy as np
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import plotly.express as px
from pathlib import Path
import warnings

warnings.filterwarnings('ignore')


def create_filtered_comparison_chart(df):
    """
    Создаёт график сравнения Арктика vs Не-Арктика с кнопками фильтрации
    """

    # Разделяем данные
    arctic_df = df[df['is_arctic'] == True].copy()
    non_arctic_df = df[df['is_arctic'] == False].copy()

    # Создаём figure
    fig = go.Figure()

    # Trace 1: Все НП (scatter plot индекс v2)
    fig.add_trace(go.Scatter(
        x=df['poad_normalized'],
        y=df['accessibility_normalized'],
        mode='markers',
        marker=dict(
            size=df['population'] / 200,
            color=df['attractiveness_v2_score_0_10'],
            colorscale='RdYlGn',
            showscale=True,
            colorbar=dict(title='Индекс v2<br>(0-10)'),
            line=dict(width=0.5, color='white')
        ),
        text=[f"<b>{row['settlement_name']}</b><br>"
              f"Индекс v2: {row['attractiveness_v2_score_0_10']:.2f}/10<br>"
              f"POAD: {row['poad_normalized']:.2f}<br>"
              f"Доступность: {row['accessibility_normalized']:.2f}<br>"
              f"Население: {row['population']:,.0f}<br>"
              f"Кластер: {row['cluster_name']}"
              for _, row in df.iterrows()],
        hoverinfo='text',
        name='Все НП',
        visible=True
    ))

    # Trace 2: Только арктические
    fig.add_trace(go.Scatter(
        x=arctic_df['poad_normalized'],
        y=arctic_df['accessibility_normalized'],
        mode='markers',
        marker=dict(
            size=arctic_df['population'] / 200,
            color=arctic_df['attractiveness_v2_score_0_10'],
            colorscale='RdYlGn',
            showscale=True,
            colorbar=dict(title='Индекс v2<br>(0-10)'),
            line=dict(width=0.5, color='white')
        ),
        text=[f"<b>{row['settlement_name']}</b><br>"
              f"Индекс v2: {row['attractiveness_v2_score_0_10']:.2f}/10<br>"
              f"POAD: {row['poad_normalized']:.2f}<br>"
              f"Доступность: {row['accessibility_normalized']:.2f}<br>"
              f"Население: {row['population']:,.0f}<br>"
              f"Кластер: {row['cluster_name']}"
              for _, row in arctic_df.iterrows()],
        hoverinfo='text',
        name='Арктические НП',
        visible=False
    ))

    # Trace 3: Только не-арктические
    fig.add_trace(go.Scatter(
        x=non_arctic_df['poad_normalized'],
        y=non_arctic_df['accessibility_normalized'],
        mode='markers',
        marker=dict(
            size=non_arctic_df['population'] / 200,
            color=non_arctic_df['attractiveness_v2_score_0_10'],
            colorscale='RdYlGn',
            showscale=True,
            colorbar=dict(title='Индекс v2<br>(0-10)'),
            line=dict(width=0.5, color='white')
        ),
        text=[f"<b>{row['settlement_name']}</b><br>"
              f"Индекс v2: {row['attractiveness_v2_score_0_10']:.2f}/10<br>"
              f"POAD: {row['poad_normalized']:.2f}<br>"
              f"Доступность: {row['accessibility_normalized']:.2f}<br>"
              f"Население: {row['population']:,.0f}<br>"
              f"Кластер: {row['cluster_name']}"
              for _, row in non_arctic_df.iterrows()],
        hoverinfo='text',
        name='Не-арктические НП',
        visible=False
    ))

    # Кнопки фильтрации
    fig.update_layout(
        updatemenus=[
            dict(
                type="buttons",
                direction="left",
                buttons=list([
                    dict(
                        args=[{"visible": [True, False, False]}],
                        label="Все НП",
                        method="update"
                    ),
                    dict(
                        args=[{"visible": [False, True, False]}],
                        label="Только Арктика",
                        method="update"
                    ),
                    dict(
                        args=[{"visible": [False, False, True]}],
                        label="Только Не-Арктика",
                        method="update"
                    ),
                ]),
                pad={"r": 10, "t": 10},
                showactive=True,
                x=0.0,
                xanchor="left",
                y=1.15,
                yanchor="top",
                bgcolor="#667eea",
                bordercolor="#667eea",
                font=dict(color='white', size=12)
            ),
        ]
    )

    fig.update_xaxes(title='POAD (нормализованный)', range=[-0.05, 1.05])
    fig.update_yaxes(title='Транспортная доступность', range=[-0.05, 1.05])

    fig.update_layout(
        title={
            'text': '🔍 POAD vs Доступность (размер = население, цвет = индекс v2)',
            'x': 0.5,
            'xanchor': 'center',
            'font': {'size': 16}
        },
        height=600,
        margin=dict(l=50, r=50, t=120, b=50),
        plot_bgcolor='rgba(250,250,250,0.5)'
    )

    return fig


def create_boxplot_comparison(df):
    """Создаёт box plot сравнение Арктика vs Не-Арктика"""

    fig = go.Figure()

    # Box plot для арктических НП
    arctic_data = df[df['is_arctic'] == True]['attractiveness_v2_score_0_10']
    non_arctic_data = df[df['is_arctic'] == False]['attractiveness_v2_score_0_10']

    fig.add_trace(go.Box(
        y=arctic_data,
        name='Арктические НП',
        marker_color='#3498db',
        boxmean='sd'
    ))

    fig.add_trace(go.Box(
        y=non_arctic_data,
        name='Не-арктические НП',
        marker_color='#e74c3c',
        boxmean='sd'
    ))

    fig.update_layout(
        title={
            'text': '📊 Сравнение распределения индекса v2: Арктика vs Не-Арктика',
            'x': 0.5,
            'xanchor': 'center',
            'font': {'size': 16}
        },
        yaxis_title='Индекс привлекательности v2 (0-10)',
        height=500,
        showlegend=True
    )

    # Добавляем аннотации со средними значениями
    arctic_mean = arctic_data.mean()
    non_arctic_mean = non_arctic_data.mean()

    fig.add_annotation(
        x=0, y=arctic_mean,
        text=f'Среднее: {arctic_mean:.2f}',
        showarrow=False,
        xshift=60,
        bgcolor='rgba(52, 152, 219, 0.8)',
        font=dict(color='white', size=10)
    )

    fig.add_annotation(
        x=1, y=non_arctic_mean,
        text=f'Среднее: {non_arctic_mean:.2f}',
        showarrow=False,
        xshift=60,
        bgcolor='rgba(231, 76, 60, 0.8)',
        font=dict(color='white', size=10)
    )

    return fig


def create_cluster_filter_chart(df):
    """Создаёт график с фильтрацией по кластерам (dropdown)"""

    cluster_names = sorted(df['cluster_name'].unique())

    fig = go.Figure()

    # Добавляем trace для каждого кластера
    for i, cluster in enumerate(cluster_names):
        cluster_df = df[df['cluster_name'] == cluster]

        visible = True if i == 0 else False

        fig.add_trace(go.Bar(
            x=['Средний индекс v2', 'Средний POAD', 'Средняя доступность'],
            y=[
                cluster_df['attractiveness_v2_score_0_10'].mean(),
                cluster_df['poad_normalized'].mean() * 10,  # нормализуем к 0-10
                cluster_df['accessibility_normalized'].mean() * 10
            ],
            name=cluster,
            visible=visible,
            marker_color='#667eea',
            text=[
                f"{cluster_df['attractiveness_v2_score_0_10'].mean():.2f}",
                f"{cluster_df['poad_normalized'].mean() * 10:.2f}",
                f"{cluster_df['accessibility_normalized'].mean() * 10:.2f}"
            ],
            textposition='outside'
        ))

    # Dropdown меню для выбора кластера
    buttons = []
    for i, cluster in enumerate(cluster_names):
        visibility = [False] * len(cluster_names)
        visibility[i] = True
        buttons.append(
            dict(
                label=cluster,
                method="update",
                args=[{"visible": visibility}]
            )
        )

    fig.update_layout(
        updatemenus=[
            dict(
                buttons=buttons,
                direction="down",
                pad={"r": 10, "t": 10},
                showactive=True,
                x=0.01,
                xanchor="left",
                y=1.15,
                yanchor="top",
                bgcolor="#ecf0f1",
                bordercolor="#bdc3c7"
            ),
        ],
        title={
            'text': '📋 Показатели по кластерам (выберите кластер)',
            'x': 0.5,
            'xanchor': 'center',
            'font': {'size': 16}
        },
        yaxis_title='Значение (0-10)',
        height=500,
        margin=dict(l=50, r=50, t=120, b=50)
    )

    return fig


def create_stats_cards(df):
    """Создаёт HTML карточки со статистикой"""

    arctic_count = df['is_arctic'].sum()
    non_arctic_count = len(df) - arctic_count
    arctic_mean_v2 = df[df['is_arctic'] == True]['attractiveness_v2_score_0_10'].mean()
    non_arctic_mean_v2 = df[df['is_arctic'] == False]['attractiveness_v2_score_0_10'].mean()

    stats_html = f"""
    <div class="stats-grid">
        <div class="stat-card arctic">
            <h3>{arctic_count}</h3>
            <p>Арктических НП</p>
            <small>Средний индекс: {arctic_mean_v2:.2f}/10</small>
        </div>
        <div class="stat-card non-arctic">
            <h3>{non_arctic_count}</h3>
            <p>Не-арктических НП</p>
            <small>Средний индекс: {non_arctic_mean_v2:.2f}/10</small>
        </div>
        <div class="stat-card diff">
            <h3>{abs(arctic_mean_v2 - non_arctic_mean_v2):.2f}</h3>
            <p>Разница индексов</p>
            <small>{'Арктика выше' if arctic_mean_v2 > non_arctic_mean_v2 else 'Не-Арктика выше'}</small>
        </div>
    </div>
    """

    return stats_html


def build_filtered_dashboard():
    """Основная функция создания дашборда с фильтрами"""

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
        comparison_df[['settlement_id', 'delta_v2_minus_v1']],
        on='settlement_id',
        how='left'
    )

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

    # Создаём компоненты
    print("\n🎨 Создание интерактивных компонентов...")

    print("   1/3 График POAD vs Доступность с кнопками фильтрации...")
    scatter_fig = create_filtered_comparison_chart(df)

    print("   2/3 Box plot сравнение Арктика vs Не-Арктика...")
    boxplot_fig = create_boxplot_comparison(df)

    print("   3/3 График по кластерам с dropdown...")
    cluster_fig = create_cluster_filter_chart(df)

    # Создаём HTML
    print("\n📦 Сборка HTML дашборда...")

    stats_cards = create_stats_cards(df)

    html_content = f"""
<!DOCTYPE html>
<html lang="ru">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>Arctic Dashboard - Интерактивные Фильтры</title>
    <style>
        body {{
            font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif;
            margin: 0;
            padding: 0;
            background: #f5f7fa;
        }}
        .header {{
            background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
            color: white;
            padding: 30px 20px;
            text-align: center;
            box-shadow: 0 2px 10px rgba(0,0,0,0.1);
        }}
        .header h1 {{
            margin: 0;
            font-size: 2.5em;
            font-weight: 700;
        }}
        .header p {{
            margin: 10px 0 0 0;
            font-size: 1.1em;
            opacity: 0.95;
        }}
        .container {{
            max-width: 1400px;
            margin: 30px auto;
            padding: 0 20px;
        }}
        .stats-grid {{
            display: grid;
            grid-template-columns: repeat(auto-fit, minmax(250px, 1fr));
            gap: 20px;
            margin: 30px 0;
        }}
        .stat-card {{
            background: white;
            padding: 25px;
            border-radius: 10px;
            text-align: center;
            box-shadow: 0 2px 8px rgba(0,0,0,0.08);
            border-left: 4px solid #667eea;
        }}
        .stat-card.arctic {{
            border-left-color: #3498db;
        }}
        .stat-card.non-arctic {{
            border-left-color: #e74c3c;
        }}
        .stat-card.diff {{
            border-left-color: #f39c12;
        }}
        .stat-card h3 {{
            margin: 0 0 10px 0;
            font-size: 2.5em;
            font-weight: 700;
            color: #2c3e50;
        }}
        .stat-card p {{
            margin: 0 0 5px 0;
            color: #7f8c8d;
            font-size: 1em;
        }}
        .stat-card small {{
            color: #95a5a6;
            font-size: 0.85em;
        }}
        .section {{
            background: white;
            border-radius: 10px;
            padding: 20px;
            margin-bottom: 30px;
            box-shadow: 0 2px 8px rgba(0,0,0,0.08);
        }}
        .info-box {{
            background: #e3f2fd;
            border-left: 4px solid #2196f3;
            padding: 15px 20px;
            margin: 20px 0;
            border-radius: 5px;
        }}
        .info-box h4 {{
            margin: 0 0 8px 0;
            color: #1976d2;
        }}
        .info-box p {{
            margin: 0;
            color: #424242;
            line-height: 1.6;
        }}
        .footer {{
            text-align: center;
            padding: 30px;
            color: #666;
            font-size: 0.9em;
        }}
    </style>
</head>
<body>
    <div class="header">
        <h1>🎯 Arctic Viability Index - Interactive Dashboard</h1>
        <p>Сравнение арктических и не-арктических населённых пунктов</p>
    </div>

    <div class="container">
        {stats_cards}

        <div class="info-box">
            <h4>💡 Как использовать фильтры:</h4>
            <p>
                <strong>График 1:</strong> Используйте кнопки "Все НП" / "Только Арктика" / "Только Не-Арктика" для фильтрации.<br>
                <strong>График 3:</strong> Выберите кластер из выпадающего списка для просмотра его показателей.
            </p>
        </div>

        <div class="section">
            {scatter_fig.to_html(full_html=False, include_plotlyjs='cdn')}
        </div>

        <div class="section">
            {boxplot_fig.to_html(full_html=False, include_plotlyjs=False)}
        </div>

        <div class="section">
            {cluster_fig.to_html(full_html=False, include_plotlyjs=False)}
        </div>
    </div>

    <div class="footer">
        <p><strong>Arctic Viability Index Dashboard</strong> | Интерактивные фильтры</p>
        <p>Дата создания: 2025-11-04 | Claude Code + Plotly</p>
    </div>
</body>
</html>
"""

    # Сохраняем
    dashboard_file = viz_dir / 'dashboard_filtered.html'
    with open(dashboard_file, 'w', encoding='utf-8') as f:
        f.write(html_content)

    print(f"\n✅ Интерактивный дашборд создан: {dashboard_file}")
    print(f"📊 Размер файла: {dashboard_file.stat().st_size / 1024:.1f} KB")

    print("\n" + "=" * 80)
    print("✅ ДАШБОРД С ФИЛЬТРАМИ ГОТОВ!")
    print("=" * 80)
    print(f"📂 Откройте файл в браузере: {dashboard_file}")
    print("   🔍 Используйте кнопки фильтрации для анализа данных")
    print("=" * 80)

    return dashboard_file


if __name__ == '__main__':
    build_filtered_dashboard()
