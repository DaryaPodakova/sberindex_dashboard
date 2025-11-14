"""
Создание простой таблицы критических НП со встроенными данными.

Использование:
    python build_simple_table.py

Результат:
    sberindex_dashboard/widgets/table_critical_settlements.html
"""

import sys
import json
from pathlib import Path

# Fix stdout encoding for Windows console
if sys.stdout.encoding != 'utf-8':
    sys.stdout.reconfigure(encoding='utf-8')

BASE_DIR = Path(__file__).resolve().parent.parent
JSON_FILE = BASE_DIR / "data" / "ndi_data.json"
TEMPLATE_FILE = BASE_DIR / "widgets" / "table_template.html"
OUTPUT_FILE = BASE_DIR / "widgets" / "table_critical_settlements.html"

print("=" * 80)
print("📊 СОЗДАНИЕ ТАБЛИЦЫ АРКТИЧЕСКИХ НП")
print("=" * 80)
print()

# Читаем JSON
print(f"📖 Читаю JSON: {JSON_FILE}")
with open(JSON_FILE, 'r', encoding='utf-8') as f:
    all_data = json.load(f)

# Сортируем по NDI (от худшего к лучшему)
all_data.sort(key=lambda x: x['ndi_10'])

critical_count = len([d for d in all_data if d['ndi_10'] < 3.0])
print(f"✅ Загружено {len(all_data)} НП, критических (NDI < 3.0): {critical_count}")

# Читаем шаблон
print(f"📖 Читаю шаблон: {TEMPLATE_FILE}")
with open(TEMPLATE_FILE, 'r', encoding='utf-8') as f:
    html_template = f.read()

# Создаем JavaScript код
js_code = f"""
// Глобальные переменные
let allData = {json.dumps(all_data, ensure_ascii=False)};
let currentSortColumn = null;
let currentSortDirection = 'asc';

// Загрузка данных
function loadData() {{
    try {{
        console.log(`Загружено ${{allData.length}} НП`);

        renderTable();
        setupSorting();

        // Показываем контент
        document.getElementById('loading').style.display = 'none';
        document.getElementById('tableWrapper').style.display = 'block';

    }} catch (error) {{
        console.error('Ошибка загрузки данных:', error);
        document.getElementById('loading').style.display = 'none';
        const errorDiv = document.getElementById('error');
        errorDiv.textContent = `Ошибка загрузки данных: ${{error.message}}`;
        errorDiv.style.display = 'block';
    }}
}}

// Настройка сортировки
function setupSorting() {{
    const headers = document.querySelectorAll('.priority-table th[data-sort]');
    headers.forEach(header => {{
        header.style.cursor = 'pointer';
        header.addEventListener('click', () => {{
            const column = header.getAttribute('data-sort');
            sortTable(column);
        }});
    }});
}}

// Сортировка таблицы
function sortTable(column) {{
    if (currentSortColumn === column) {{
        currentSortDirection = currentSortDirection === 'asc' ? 'desc' : 'asc';
    }} else {{
        currentSortColumn = column;
        currentSortDirection = 'asc';
    }}

    allData.sort((a, b) => {{
        let aVal = a[column];
        let bVal = b[column];

        // Handle null values
        if (aVal === null) return 1;
        if (bVal === null) return -1;

        // String comparison
        if (typeof aVal === 'string') {{
            return currentSortDirection === 'asc'
                ? aVal.localeCompare(bVal, 'ru')
                : bVal.localeCompare(aVal, 'ru');
        }}

        // Numeric comparison
        return currentSortDirection === 'asc' ? aVal - bVal : bVal - aVal;
    }});

    renderTable();
}}

// Создание tooltip текста
function createTooltip(settlement) {{
    const arcticStatus = settlement.is_arctic ? '✅ Да' : '❌ Нет';
    return `📍 НП: ${{settlement.settlement_name}}
📊 NDI (0-10): ${{settlement.ndi_10.toFixed(2)}}
🗺️ Регион: ${{settlement.region_name}}
🏘️ Тип: ${{settlement.settlement_type}}
🏔️ Арктика: ${{arcticStatus}}

Компоненты NDI (баллы из 100):
• Качество жизни: ${{settlement.poad_score_100.toFixed(1)}}
• Доступность рынков: ${{settlement.market_score_100.toFixed(1)}}
• Потребление: ${{settlement.consumption_score_100.toFixed(1)}}
• Доступность инфраструктуры: ${{settlement.accessibility_score_100.toFixed(1)}}
• Климат: ${{settlement.climate_score_100.toFixed(1)}}
• Мобильность: ${{settlement.mobility_score_100.toFixed(1)}}

Температура:
• Зима: ${{settlement.avg_temp_winter_celsius !== null ? settlement.avg_temp_winter_celsius.toFixed(1) + '°C' : 'нет данных'}}
• Лето: ${{settlement.avg_temp_summer_celsius !== null ? settlement.avg_temp_summer_celsius.toFixed(1) + '°C' : 'нет данных'}}
• Амплитуда: ${{settlement.temp_amplitude_celsius !== null ? settlement.temp_amplitude_celsius.toFixed(1) + '°C' : 'нет данных'}}`;
}}

// Отрисовка таблицы
function renderTable() {{
    const tbody = document.getElementById('criticalTableBody');
    tbody.innerHTML = '';

    allData.forEach((d, i) => {{
        const tr = document.createElement('tr');

        // Tooltip при наведении
        tr.title = createTooltip(d);
        tr.onclick = () => highlightSettlement(d);

        const arcticBadge = d.is_arctic ?
            '<span class="badge badge-arctic">ДА</span>' :
            '<span class="badge">НЕТ</span>';

        const amplitude = d.temp_amplitude_celsius !== null ?
            d.temp_amplitude_celsius.toFixed(1) :
            '—';

        const extremes = (d.avg_temp_winter_celsius !== null && d.avg_temp_summer_celsius !== null) ?
            `<span class="temp-cold">${{d.avg_temp_winter_celsius.toFixed(1)}}</span> / <span class="temp-hot">${{d.avg_temp_summer_celsius.toFixed(1)}}</span>` :
            '—';

        // Динамическая окраска NDI на основе color_ndi
        const ndiColor = d.color_ndi || '#e74c3c';

        tr.innerHTML = `
            <td>${{i + 1}}</td>
            <td><strong>${{d.settlement_name}}</strong></td>
            <td>${{arcticBadge}}</td>
            <td class="score-cell"><strong style="color:${{ndiColor}}">${{d.ndi_10.toFixed(2)}}</strong></td>
            <td class="score-cell">${{d.poad_score_100.toFixed(1)}}</td>
            <td class="score-cell">${{d.market_score_100.toFixed(1)}}</td>
            <td class="score-cell">${{d.consumption_score_100.toFixed(1)}}</td>
            <td class="score-cell">${{d.accessibility_score_100.toFixed(1)}}</td>
            <td class="score-cell">${{d.climate_score_100.toFixed(1)}}</td>
            <td class="score-cell">${{d.mobility_score_100.toFixed(1)}}</td>
            <td class="score-cell">${{amplitude}}</td>
            <td class="score-cell">${{extremes}}</td>
            <td>${{d.region_name}}</td>
        `;
        tbody.appendChild(tr);
    }});
}}

// Подсветка выбранного НП (клик)
function highlightSettlement(settlement) {{
    console.log('Выбран НП:', settlement.settlement_name);
    alert(createTooltip(settlement));
}}

// Загрузка при открытии страницы
window.addEventListener('DOMContentLoaded', loadData);
"""

# Заменяем placeholder
html_output = html_template.replace('// PLACEHOLDER_FOR_DATA', js_code)

# Сохраняем
print(f"💾 Сохраняю HTML: {OUTPUT_FILE}")
with open(OUTPUT_FILE, 'w', encoding='utf-8') as f:
    f.write(html_output)

file_size = OUTPUT_FILE.stat().st_size
print(f"✅ Файл создан: {OUTPUT_FILE}")
print(f"📊 Размер: {file_size:,} байт ({file_size / 1024:.1f} KB)")
print()
print("=" * 80)
print("✅ ГОТОВО! Таблица готова к использованию")
print("=" * 80)
print()
print("📂 Откройте файл в браузере:")
print(f"   {OUTPUT_FILE}")
