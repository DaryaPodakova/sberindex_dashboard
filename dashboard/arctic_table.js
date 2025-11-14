// ============================================================================
// ARCTIC TABLE - Таблица населённых пунктов арктической зоны
// ============================================================================
// Автор: Claude Code
// Дата: 2025-01-14
// Описание: Интерактивная таблица со всеми населёнными пунктами и сортировкой

// Глобальные переменные
let arcticTableData = [];
let arcticFilteredData = [];
let currentSortColumn = null;
let currentSortDirection = 'asc';
let arcticRegionFilter = 'all';
let arcticZoneFilter = 'all';

// ============================================================================
// ЗАГРУЗКА ДАННЫХ
// ============================================================================
async function loadArcticTableData() {
    try {
        const response = await fetch('ndi_data.json');
        if (!response.ok) {
            throw new Error(`HTTP error! status: ${response.status}`);
        }
        const rawData = await response.json();

        // Преобразуем данные в нужный формат с процентами
        arcticTableData = rawData.map(d => ({
            ...d,
            poad_score_100: d.poad_score * 100,
            market_score_100: d.market_score * 100,
            consumption_score_100: d.consumption_score * 100,
            accessibility_score_100: d.accessibility_score * 100,
            climate_score_100: d.climate_score * 100,
            mobility_score_100: d.mobility_score * 100,
            // Определяем цвет на основе NDI
            color_ndi: d.ndi_10 < 3.0 ? '#d32f2f' :
                       d.ndi_10 < 4.5 ? '#f57c00' :
                       d.ndi_10 < 6.5 ? '#fbc02d' : '#388e3c',
            // Используем реальное значение is_arctic из данных
            is_arctic: Boolean(d.is_arctic)
        }));

        console.log(`✅ Arctic Table: Загружено ${arcticTableData.length} населённых пунктов`);

        // Отладка: проверяем значения is_arctic
        const arcticCount = arcticTableData.filter(d => d.is_arctic === true).length;
        const nonArcticCount = arcticTableData.filter(d => d.is_arctic === false).length;
        console.log(`   Арктических: ${arcticCount}, Не арктических: ${nonArcticCount}`);

        // Показываем примеры значений
        if (arcticTableData.length > 0) {
            console.log(`   Пример первой записи is_arctic:`, arcticTableData[0].is_arctic, typeof arcticTableData[0].is_arctic);
        }

        // Инициализируем отфильтрованные данные
        arcticFilteredData = [...arcticTableData];

        // Рендерим таблицу
        renderArcticTable();
        setupArcticTableSorting();
        setupArcticTableFilters();

        // Показываем таблицу
        document.getElementById('loading-table').style.display = 'none';
        document.getElementById('tableWrapper').style.display = 'block';

    } catch (error) {
        console.error('❌ Ошибка загрузки данных для таблицы:', error);
        document.getElementById('loading-table').style.display = 'none';
        const errorDiv = document.getElementById('error-table');
        errorDiv.textContent = `Ошибка загрузки данных: ${error.message}`;
        errorDiv.style.display = 'block';
    }
}

// ============================================================================
// НАСТРОЙКА СОРТИРОВКИ
// ============================================================================
function setupArcticTableSorting() {
    const headers = document.querySelectorAll('#arcticTable th[data-sort]');
    headers.forEach(header => {
        header.style.cursor = 'pointer';
        header.addEventListener('click', () => {
            const column = header.getAttribute('data-sort');
            sortArcticTable(column);
        });
    });
}

// ============================================================================
// СОРТИРОВКА ТАБЛИЦЫ
// ============================================================================
function sortArcticTable(column) {
    if (currentSortColumn === column) {
        currentSortDirection = currentSortDirection === 'asc' ? 'desc' : 'asc';
    } else {
        currentSortColumn = column;
        currentSortDirection = 'asc';
    }

    arcticFilteredData.sort((a, b) => {
        let aVal = a[column];
        let bVal = b[column];

        // Handle null values
        if (aVal === null) return 1;
        if (bVal === null) return -1;

        // Boolean comparison (is_arctic)
        if (typeof aVal === 'boolean') {
            return currentSortDirection === 'asc'
                ? (aVal === bVal ? 0 : aVal ? -1 : 1)
                : (aVal === bVal ? 0 : aVal ? 1 : -1);
        }

        // String comparison
        if (typeof aVal === 'string') {
            return currentSortDirection === 'asc'
                ? aVal.localeCompare(bVal, 'ru')
                : bVal.localeCompare(aVal, 'ru');
        }

        // Numeric comparison
        return currentSortDirection === 'asc'
            ? aVal - bVal
            : bVal - aVal;
    });

    renderArcticTable();
}

// ============================================================================
// СОЗДАНИЕ TOOLTIP
// ============================================================================
function createArcticTooltip(settlement) {
    const winterTemp = settlement.avg_temp_winter_celsius !== null && settlement.avg_temp_winter_celsius !== undefined
        ? settlement.avg_temp_winter_celsius.toFixed(1) + '°C'
        : 'нет данных';

    const summerTemp = settlement.avg_temp_summer_celsius !== null && settlement.avg_temp_summer_celsius !== undefined
        ? settlement.avg_temp_summer_celsius.toFixed(1) + '°C'
        : 'нет данных';

    const amplitude = settlement.temp_amplitude_celsius !== null && settlement.temp_amplitude_celsius !== undefined
        ? settlement.temp_amplitude_celsius.toFixed(1) + '°C'
        : 'нет данных';

    return `${settlement.settlement_name}

Регион: ${settlement.region_name}
Тип: ${settlement.settlement_type || 'не указан'}
Арктика: ${settlement.is_arctic ? 'Да' : 'Нет'}

Показатели (0-100):
• NDI (0-10): ${settlement.ndi_10.toFixed(2)}
• Качество жизни: ${settlement.poad_score_100.toFixed(1)}
• Доступность рынков: ${settlement.market_score_100.toFixed(1)}
• Потребление: ${settlement.consumption_score_100.toFixed(1)}
• Доступность инфраструктуры: ${settlement.accessibility_score_100.toFixed(1)}
• Климат: ${settlement.climate_score_100.toFixed(1)}
• Мобильность: ${settlement.mobility_score_100.toFixed(1)}

Температура:
• Зима: ${winterTemp}
• Лето: ${summerTemp}
• Амплитуда: ${amplitude}`;
}

// ============================================================================
// ФИЛЬТРАЦИЯ ТАБЛИЦЫ
// ============================================================================
function applyArcticFilters() {
    console.log(`🔍 Применяем фильтры: регион="${arcticRegionFilter}", арктика="${arcticZoneFilter}"`);

    arcticFilteredData = arcticTableData.filter(d => {
        // Фильтр по региону
        if (arcticRegionFilter !== 'all' && d.region_name !== arcticRegionFilter) {
            return false;
        }
        // Фильтр по арктической зоне
        if (arcticZoneFilter === 'yes' && d.is_arctic !== true) {
            return false;
        }
        if (arcticZoneFilter === 'no' && d.is_arctic !== false) {
            return false;
        }
        return true;
    });

    console.log(`   Результат: ${arcticFilteredData.length} записей из ${arcticTableData.length}`);
    renderArcticTable();
}

// ============================================================================
// ОТРИСОВКА ТАБЛИЦЫ
// ============================================================================
function renderArcticTable() {
    const tbody = document.getElementById('arcticTableBody');
    if (!tbody) return;

    tbody.innerHTML = '';

    arcticFilteredData.forEach((d, i) => {
        const tr = document.createElement('tr');

        // Tooltip при наведении
        tr.title = createArcticTooltip(d);
        tr.onclick = () => highlightArcticSettlement(d);

        const arcticBadge = d.is_arctic ?
            '<span class="badge badge-arctic">ДА</span>' :
            '<span class="badge">НЕТ</span>';

        const amplitude = d.temp_amplitude_celsius !== null && d.temp_amplitude_celsius !== undefined
            ? d.temp_amplitude_celsius.toFixed(1)
            : '—';

        let extremes = '—';
        if (d.avg_temp_winter_celsius !== null && d.avg_temp_winter_celsius !== undefined &&
            d.avg_temp_summer_celsius !== null && d.avg_temp_summer_celsius !== undefined) {
            extremes = `<span class="temp-cold">${d.avg_temp_winter_celsius.toFixed(1)}</span> / <span class="temp-hot">${d.avg_temp_summer_celsius.toFixed(1)}</span>`;
        }

        // Динамическая окраска NDI
        const ndiColor = d.color_ndi || '#e74c3c';

        tr.innerHTML = `
            <td>${i + 1}</td>
            <td><strong>${d.settlement_name}</strong></td>
            <td>${arcticBadge}</td>
            <td class="score-cell"><strong style="color:${ndiColor}">${d.ndi_10.toFixed(2)}</strong></td>
            <td class="score-cell">${d.poad_score_100.toFixed(1)}</td>
            <td class="score-cell">${d.market_score_100.toFixed(1)}</td>
            <td class="score-cell">${d.consumption_score_100.toFixed(1)}</td>
            <td class="score-cell">${d.accessibility_score_100.toFixed(1)}</td>
            <td class="score-cell">${d.climate_score_100.toFixed(1)}</td>
            <td class="score-cell">${d.mobility_score_100.toFixed(1)}</td>
            <td class="score-cell">${amplitude}</td>
            <td class="score-cell">${extremes}</td>
            <td>${d.region_name}</td>
        `;
        tbody.appendChild(tr);
    });
}

// ============================================================================
// ПОДСВЕТКА ВЫБРАННОГО НП
// ============================================================================
function highlightArcticSettlement(settlement) {
    console.log('Выбран НП:', settlement.settlement_name);
    alert(createArcticTooltip(settlement));
}

// ============================================================================
// НАСТРОЙКА ФИЛЬТРОВ
// ============================================================================
function setupArcticTableFilters() {
    const tableRegionFilter = document.getElementById('table-region-filter');
    const arcticFilter = document.getElementById('arctic-filter');
    const regionFilter = document.getElementById('region-filter');

    // Синхронизация фильтра региона таблицы с фильтром виджета
    if (tableRegionFilter) {
        tableRegionFilter.addEventListener('change', (e) => {
            arcticRegionFilter = e.target.value;
            // Синхронизируем с главным фильтром региона
            if (regionFilter) {
                regionFilter.value = e.target.value;
                // Триггерим изменение для виджета
                regionFilter.dispatchEvent(new Event('change'));
            }
            applyArcticFilters();

            // Обновляем состояние кнопок "Сбросить всё"
            if (typeof updateResetAllButtons === 'function') {
                updateResetAllButtons();
            }
        });
    }

    // Фильтр арктической зоны
    if (arcticFilter) {
        arcticFilter.addEventListener('change', (e) => {
            arcticZoneFilter = e.target.value;
            applyArcticFilters();

            // Обновляем состояние кнопок "Сбросить всё"
            if (typeof updateResetAllButtons === 'function') {
                updateResetAllButtons();
            }
        });
    }

    // Обратная синхронизация - когда меняют фильтр виджета, меняем и таблицу
    if (regionFilter) {
        regionFilter.addEventListener('change', (e) => {
            arcticRegionFilter = e.target.value;
            if (tableRegionFilter) {
                tableRegionFilter.value = e.target.value;
            }
            applyArcticFilters();
        });
    }
}

// ============================================================================
// ИНИЦИАЛИЗАЦИЯ
// ============================================================================
document.addEventListener('DOMContentLoaded', () => {
    // Загружаем данные для таблицы
    loadArcticTableData();
});
