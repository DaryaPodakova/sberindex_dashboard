// ============================================================================
// NDI DECOMPOSITION WIDGET - Виджет декомпозиции NDI
// ============================================================================
// Автор: Claude Code
// Дата: 2025-01-14
// Описание: Интерактивный виджет для декомпозиции и сравнения компонентов NDI

// Глобальные переменные виджета
let ndiWidgetData = [];
let ndiSelectedSettlement = 'average';
let ndiCurrentCompareMode = 'vs-russia';
let ndiSelectedRegion = 'all';

// Функция обновления состояния кнопок "Сбросить всё"
function updateResetAllButtons() {
    const resetAllFilters = document.getElementById('reset-all-filters');
    const tableResetAllFilters = document.getElementById('table-reset-all-filters');

    const settlementSearch = document.getElementById('settlement-search');
    const settlementSelect = document.getElementById('settlement-select');
    const compareMode = document.getElementById('compare-mode');
    const regionFilter = document.getElementById('region-filter');
    const tableSearch = document.getElementById('table-search');
    const tableSettlementSelect = document.getElementById('table-settlement-select');
    const tableRegionFilter = document.getElementById('table-region-filter');
    const arcticFilter = document.getElementById('arctic-filter');

    // Проверяем, есть ли активные фильтры
    const isActive = (
        (settlementSearch && settlementSearch.value !== '') ||
        (settlementSelect && settlementSelect.selectedIndex !== 0) ||
        (compareMode && compareMode.value !== 'vs-russia') ||
        (regionFilter && regionFilter.value !== 'all') ||
        (tableSearch && tableSearch.value !== '') ||
        (tableSettlementSelect && tableSettlementSelect.selectedIndex !== 0) ||
        (tableRegionFilter && tableRegionFilter.value !== 'all') ||
        (arcticFilter && arcticFilter.value !== 'all')
    );

    // Обновляем состояние кнопок
    if (resetAllFilters) {
        if (isActive) {
            resetAllFilters.classList.add('active');
        } else {
            resetAllFilters.classList.remove('active');
        }
    }

    if (tableResetAllFilters) {
        if (isActive) {
            tableResetAllFilters.classList.add('active');
        } else {
            tableResetAllFilters.classList.remove('active');
        }
    }
}

// ============================================================================
// ЗАГРУЗКА ДАННЫХ
// ============================================================================
async function loadNDIWidgetData() {
    try {
        const response = await fetch('ndi_data.json');
        if (!response.ok) {
            throw new Error(`HTTP error! status: ${response.status}`);
        }
        ndiWidgetData = await response.json();
        console.log(`✅ NDI Widget: Загружено ${ndiWidgetData.length} населённых пунктов`);

        // Инициализация виджета после загрузки данных
        initializeNDIWidget();
    } catch (error) {
        console.error('❌ Ошибка загрузки ndi_data.json:', error);
        const waterfallDiv = document.getElementById('waterfall');
        if (waterfallDiv) {
            waterfallDiv.innerHTML = `
                <div style="padding: 40px; text-align: center; color: #e74c3c;">
                    <h3>⚠️ Ошибка загрузки данных</h3>
                    <p>Не удалось загрузить файл ndi_data.json</p>
                    <p style="font-size: 12px; color: #95a5a6;">${error.message}</p>
                </div>
            `;
        }
    }
}

// ============================================================================
// ИНИЦИАЛИЗАЦИЯ
// ============================================================================
function initializeNDIWidget() {
    populateRegionDropdowns();
    populateNDIDropdown();
    renderNDIWaterfall(ndiSelectedSettlement, ndiCurrentCompareMode);
    renderNDIRadar(ndiCurrentCompareMode);
    updateNDIResetButton();
}

// Заполнение выпадающего списка НП
function populateNDIDropdown() {
    const select = document.getElementById('settlement-select');
    const tableSelect = document.getElementById('table-settlement-select');

    if (!select) return;

    // Очищаем все опции кроме первой
    while (select.options.length > 1) {
        select.remove(1);
    }
    if (tableSelect) {
        while (tableSelect.options.length > 1) {
            tableSelect.remove(1);
        }
    }

    // Фильтруем данные по выбранному региону
    const filteredData = ndiSelectedRegion === 'all'
        ? ndiWidgetData
        : ndiWidgetData.filter(s => s.region_name === ndiSelectedRegion);

    filteredData.forEach((s, idx) => {
        const originalIdx = ndiWidgetData.indexOf(s);
        const option = document.createElement('option');
        option.value = originalIdx;
        option.textContent = `${s.settlement_name} (${s.region_name}) — NDI: ${s.ndi_10.toFixed(2)}`;
        select.appendChild(option);

        if (tableSelect) {
            const option2 = option.cloneNode(true);
            tableSelect.appendChild(option2);
        }
    });
}

// Заполнение выпадающих списков регионов
function populateRegionDropdowns() {
    const regionFilter = document.getElementById('region-filter');
    const tableRegionFilter = document.getElementById('table-region-filter');

    if (!regionFilter) return;

    // Получаем уникальные регионы
    const regions = [...new Set(ndiWidgetData.map(d => d.region_name))].sort();

    regions.forEach(region => {
        const option = document.createElement('option');
        option.value = region;
        option.textContent = region;
        regionFilter.appendChild(option);

        if (tableRegionFilter) {
            const option2 = option.cloneNode(true);
            tableRegionFilter.appendChild(option2);
        }
    });
}

// ============================================================================
// WATERFALL CHART
// ============================================================================
function renderNDIWaterfall(settlement, mode) {
    // Определяем базовую линию
    let baseNDI, basePoad, baseMarket, baseConsumption, baseAccess, baseClimate, baseMobility, baseName;

    if (mode === 'vs-russia') {
        baseNDI = ndiWidgetData.reduce((sum, d) => sum + d.ndi_10, 0) / ndiWidgetData.length;
        basePoad = ndiWidgetData.reduce((sum, d) => sum + d.poad_score, 0) / ndiWidgetData.length;
        baseMarket = ndiWidgetData.reduce((sum, d) => sum + d.market_score, 0) / ndiWidgetData.length;
        baseConsumption = ndiWidgetData.reduce((sum, d) => sum + d.consumption_score, 0) / ndiWidgetData.length;
        baseAccess = ndiWidgetData.reduce((sum, d) => sum + d.accessibility_score, 0) / ndiWidgetData.length;
        baseClimate = ndiWidgetData.reduce((sum, d) => sum + d.climate_score, 0) / ndiWidgetData.length;
        baseMobility = ndiWidgetData.reduce((sum, d) => sum + d.mobility_score, 0) / ndiWidgetData.length;
        baseName = 'Средний (РФ)';
    } else if (mode === 'vs-region' && settlement !== 'average') {
        const regionNPs = ndiWidgetData.filter(d => d.region_name === settlement.region_name);
        baseNDI = regionNPs.reduce((sum, d) => sum + d.ndi_10, 0) / regionNPs.length;
        basePoad = regionNPs.reduce((sum, d) => sum + d.poad_score, 0) / regionNPs.length;
        baseMarket = regionNPs.reduce((sum, d) => sum + d.market_score, 0) / regionNPs.length;
        baseConsumption = regionNPs.reduce((sum, d) => sum + d.consumption_score, 0) / regionNPs.length;
        baseAccess = regionNPs.reduce((sum, d) => sum + d.accessibility_score, 0) / regionNPs.length;
        baseClimate = regionNPs.reduce((sum, d) => sum + d.climate_score, 0) / regionNPs.length;
        baseMobility = regionNPs.reduce((sum, d) => sum + d.mobility_score, 0) / regionNPs.length;
        baseName = `Средний (${settlement.region_name})`;
    } else {
        baseNDI = ndiWidgetData.reduce((sum, d) => sum + d.ndi_10, 0) / ndiWidgetData.length;
        basePoad = ndiWidgetData.reduce((sum, d) => sum + d.poad_score, 0) / ndiWidgetData.length;
        baseMarket = ndiWidgetData.reduce((sum, d) => sum + d.market_score, 0) / ndiWidgetData.length;
        baseConsumption = ndiWidgetData.reduce((sum, d) => sum + d.consumption_score, 0) / ndiWidgetData.length;
        baseAccess = ndiWidgetData.reduce((sum, d) => sum + d.accessibility_score, 0) / ndiWidgetData.length;
        baseClimate = ndiWidgetData.reduce((sum, d) => sum + d.climate_score, 0) / ndiWidgetData.length;
        baseMobility = ndiWidgetData.reduce((sum, d) => sum + d.mobility_score, 0) / ndiWidgetData.length;
        baseName = 'Средний (РФ)';
    }

    // Определяем значения выбранного НП
    const isAverage = settlement === 'average';
    let poadContrib, marketContrib, consumptionContrib, accessContrib, climateContrib, mobilityContrib, settlementName, ndiValue;

    if (isAverage) {
        const avgPoad = ndiWidgetData.reduce((sum, d) => sum + d.poad_score, 0) / ndiWidgetData.length;
        const avgMarket = ndiWidgetData.reduce((sum, d) => sum + d.market_score, 0) / ndiWidgetData.length;
        const avgConsumption = ndiWidgetData.reduce((sum, d) => sum + d.consumption_score, 0) / ndiWidgetData.length;
        const avgAccess = ndiWidgetData.reduce((sum, d) => sum + d.accessibility_score, 0) / ndiWidgetData.length;
        const avgClimate = ndiWidgetData.reduce((sum, d) => sum + d.climate_score, 0) / ndiWidgetData.length;
        const avgMobility = ndiWidgetData.reduce((sum, d) => sum + d.mobility_score, 0) / ndiWidgetData.length;
        const avgNDI = ndiWidgetData.reduce((sum, d) => sum + d.ndi_10, 0) / ndiWidgetData.length;

        poadContrib = avgPoad * 0.35 * 10;
        marketContrib = avgMarket * 0.20 * 10;
        consumptionContrib = avgConsumption * 0.15 * 10;
        accessContrib = avgAccess * 0.15 * 10;
        climateContrib = avgClimate * 0.10 * 10;
        mobilityContrib = avgMobility * 0.05 * 10;
        settlementName = 'Все НП (среднее)';
        ndiValue = avgNDI;
    } else {
        poadContrib = settlement.poad_score * 0.35 * 10;
        marketContrib = settlement.market_score * 0.20 * 10;
        consumptionContrib = settlement.consumption_score * 0.15 * 10;
        accessContrib = settlement.accessibility_score * 0.15 * 10;
        climateContrib = settlement.climate_score * 0.10 * 10;
        mobilityContrib = settlement.mobility_score * 0.05 * 10;
        settlementName = settlement.settlement_name;
        ndiValue = settlement.ndi_10;
    }

    function getNDIColor(ndi) {
        if (ndi < 3.0) return '#d32f2f';
        if (ndi < 4.5) return '#f57c00';
        if (ndi < 6.5) return '#fbc02d';
        return '#388e3c';
    }

    const baseColor = 'rgba(128, 128, 128, 0.2)';
    const totalColor = isAverage ? baseColor : getNDIColor(ndiValue);

    const trace = {
        type: 'waterfall',
        orientation: 'v',
        measure: ['absolute', 'relative', 'relative', 'relative', 'relative', 'relative', 'relative', 'total'],
        x: [baseName, 'POAD', 'Рынок', 'Потребл.', 'Доступн.', 'Климат', 'Мобильн.', 'Итого'],
        y: [
            baseNDI,
            poadContrib - baseNDI * 0.35,
            marketContrib - baseNDI * 0.20,
            consumptionContrib - baseNDI * 0.15,
            accessContrib - baseNDI * 0.15,
            climateContrib - baseNDI * 0.10,
            mobilityContrib - baseNDI * 0.05,
            ndiValue
        ],
        text: [
            baseNDI.toFixed(1),
            (poadContrib - baseNDI * 0.35).toFixed(1),
            (marketContrib - baseNDI * 0.20).toFixed(1),
            (consumptionContrib - baseNDI * 0.15).toFixed(1),
            (accessContrib - baseNDI * 0.15).toFixed(1),
            (climateContrib - baseNDI * 0.10).toFixed(1),
            (mobilityContrib - baseNDI * 0.05).toFixed(1),
            ndiValue.toFixed(1)
        ],
        textposition: 'outside',
        connector: { line: { color: 'rgb(63, 63, 63)' } },
        increasing: { marker: { color: '#27ae60' } },
        decreasing: { marker: { color: '#e74c3c' } },
        totals: { marker: { color: totalColor } },
        marker: {
            color: [baseColor, null, null, null, null, null, null, totalColor]
        }
    };

    const layout = {
        title: `Декомпозиция NDI: ${settlementName}`,
        height: 450,
        plot_bgcolor: 'white',
        paper_bgcolor: 'white',
        font: { family: 'Segoe UI, Arial, sans-serif', size: 12, color: '#2c3e50' },
        margin: { l: 50, r: 20, t: 60, b: 60 },
        showlegend: false,
        xaxis: { gridcolor: '#ecf0f1', linecolor: '#bdc3c7' },
        yaxis: { gridcolor: '#ecf0f1', linecolor: '#bdc3c7', title: 'Баллы (0-10)' }
    };

    Plotly.newPlot('waterfall', [trace], layout, { responsive: true });
}

// ============================================================================
// RADAR CHART
// ============================================================================
function getNDIColorRgba(ndi, opacity) {
    let baseColor;
    if (ndi < 3.0) {
        baseColor = '211, 47, 47';
    } else if (ndi < 4.5) {
        baseColor = '245, 124, 0';
    } else if (ndi < 6.5) {
        baseColor = '251, 192, 45';
    } else {
        baseColor = '56, 142, 60';
    }
    return `rgba(${baseColor}, ${opacity})`;
}

function renderNDIRadar(mode) {
    let trace1, trace2;

    if (mode === 'vs-russia') {
        const avgRussia = {
            poad: ndiWidgetData.reduce((s, d) => s + d.poad_score, 0) / ndiWidgetData.length,
            market: ndiWidgetData.reduce((s, d) => s + d.market_score, 0) / ndiWidgetData.length,
            consumption: ndiWidgetData.reduce((s, d) => s + d.consumption_score, 0) / ndiWidgetData.length,
            accessibility: ndiWidgetData.reduce((s, d) => s + d.accessibility_score, 0) / ndiWidgetData.length,
            climate: ndiWidgetData.reduce((s, d) => s + d.climate_score, 0) / ndiWidgetData.length,
            mobility: ndiWidgetData.reduce((s, d) => s + d.mobility_score, 0) / ndiWidgetData.length,
            ndi: ndiWidgetData.reduce((sum, d) => sum + d.ndi_10, 0) / ndiWidgetData.length
        };

        let selectedNP, settlementName, ndiValue;

        if (ndiSelectedSettlement === 'average') {
            selectedNP = avgRussia;
            settlementName = 'Все НП (среднее)';
            ndiValue = avgRussia.ndi;
        } else {
            selectedNP = {
                poad: ndiSelectedSettlement.poad_score,
                market: ndiSelectedSettlement.market_score,
                consumption: ndiSelectedSettlement.consumption_score,
                accessibility: ndiSelectedSettlement.accessibility_score,
                climate: ndiSelectedSettlement.climate_score,
                mobility: ndiSelectedSettlement.mobility_score
            };
            settlementName = ndiSelectedSettlement.settlement_name;
            ndiValue = ndiSelectedSettlement.ndi_10;
        }

        const npColor = getNDIColorRgba(ndiValue, 0.5);

        trace1 = {
            type: 'scatterpolar',
            r: [selectedNP.poad, selectedNP.market, selectedNP.consumption, selectedNP.accessibility, selectedNP.climate, selectedNP.mobility],
            theta: ['POAD', 'Рынок', 'Потребл.', 'Доступн.', 'Климат', 'Мобильн.'],
            fill: 'toself',
            name: settlementName,
            line: { color: npColor, width: 2 },
            fillcolor: npColor,
            hovertemplate: '<b>%{fullData.name}</b> (NDI: ' + ndiValue.toFixed(2) + ')<br><br>' +
                          '<b>POAD:</b> ' + selectedNP.poad.toFixed(3) + '<br>' +
                          '<b>Рынок:</b> ' + selectedNP.market.toFixed(3) + '<br>' +
                          '<b>Потребление:</b> ' + selectedNP.consumption.toFixed(3) + '<br>' +
                          '<b>Доступность:</b> ' + selectedNP.accessibility.toFixed(3) + '<br>' +
                          '<b>Климат:</b> ' + selectedNP.climate.toFixed(3) + '<br>' +
                          '<b>Мобильность:</b> ' + selectedNP.mobility.toFixed(3) + '<extra></extra>'
        };

        trace2 = {
            type: 'scatterpolar',
            r: [avgRussia.poad, avgRussia.market, avgRussia.consumption, avgRussia.accessibility, avgRussia.climate, avgRussia.mobility],
            theta: ['POAD', 'Рынок', 'Потребл.', 'Доступн.', 'Климат', 'Мобильн.'],
            fill: 'toself',
            name: 'Среднее по РФ',
            line: { color: 'rgba(128, 128, 128, 0.2)', width: 2 },
            fillcolor: 'rgba(128, 128, 128, 0.2)',
            hovertemplate: '<b>%{fullData.name}</b> (NDI: ' + avgRussia.ndi.toFixed(2) + ')<br><br>' +
                          '<b>POAD:</b> ' + avgRussia.poad.toFixed(3) + '<br>' +
                          '<b>Рынок:</b> ' + avgRussia.market.toFixed(3) + '<br>' +
                          '<b>Потребление:</b> ' + avgRussia.consumption.toFixed(3) + '<br>' +
                          '<b>Доступность:</b> ' + avgRussia.accessibility.toFixed(3) + '<br>' +
                          '<b>Климат:</b> ' + avgRussia.climate.toFixed(3) + '<br>' +
                          '<b>Мобильность:</b> ' + avgRussia.mobility.toFixed(3) + '<extra></extra>'
        };
    } else if (mode === 'vs-region') {
        if (ndiSelectedSettlement === 'average') {
            const regions = {};
            ndiWidgetData.forEach(d => {
                if (!regions[d.region_name]) regions[d.region_name] = [];
                regions[d.region_name].push(d);
            });

            const regionStats = Object.keys(regions).map(regionName => {
                const regionData = regions[regionName];
                const avgNDI = regionData.reduce((sum, d) => sum + d.ndi_10, 0) / regionData.length;
                return {
                    name: regionName,
                    avgNDI: avgNDI,
                    count: regionData.length,
                    avgPoad: regionData.reduce((s, d) => s + d.poad_score, 0) / regionData.length,
                    avgMarket: regionData.reduce((s, d) => s + d.market_score, 0) / regionData.length,
                    avgConsumption: regionData.reduce((s, d) => s + d.consumption_score, 0) / regionData.length,
                    avgAccessibility: regionData.reduce((s, d) => s + d.accessibility_score, 0) / regionData.length,
                    avgClimate: regionData.reduce((s, d) => s + d.climate_score, 0) / regionData.length,
                    avgMobility: regionData.reduce((s, d) => s + d.mobility_score, 0) / regionData.length
                };
            }).sort((a, b) => b.avgNDI - a.avgNDI);

            const bestRegion = regionStats[0];
            const avgRussia = {
                poad: ndiWidgetData.reduce((s, d) => s + d.poad_score, 0) / ndiWidgetData.length,
                market: ndiWidgetData.reduce((s, d) => s + d.market_score, 0) / ndiWidgetData.length,
                consumption: ndiWidgetData.reduce((s, d) => s + d.consumption_score, 0) / ndiWidgetData.length,
                accessibility: ndiWidgetData.reduce((s, d) => s + d.accessibility_score, 0) / ndiWidgetData.length,
                climate: ndiWidgetData.reduce((s, d) => s + d.climate_score, 0) / ndiWidgetData.length,
                mobility: ndiWidgetData.reduce((s, d) => s + d.mobility_score, 0) / ndiWidgetData.length,
                ndi: ndiWidgetData.reduce((sum, d) => sum + d.ndi_10, 0) / ndiWidgetData.length
            };

            const bestRegionColor = getNDIColorRgba(bestRegion.avgNDI, 0.5);

            trace1 = {
                type: 'scatterpolar',
                r: [bestRegion.avgPoad, bestRegion.avgMarket, bestRegion.avgConsumption, bestRegion.avgAccessibility, bestRegion.avgClimate, bestRegion.avgMobility],
                theta: ['POAD', 'Рынок', 'Потребл.', 'Доступн.', 'Климат', 'Мобильн.'],
                fill: 'toself',
                name: `Лучший регион: ${bestRegion.name} (${bestRegion.count} НП)`,
                line: { color: bestRegionColor, width: 2 },
                fillcolor: bestRegionColor,
                hovertemplate: '<b>%{fullData.name}</b> (NDI: ' + bestRegion.avgNDI.toFixed(2) + ')<br><br>' +
                              '<b>POAD:</b> ' + bestRegion.avgPoad.toFixed(3) + '<br>' +
                              '<b>Рынок:</b> ' + bestRegion.avgMarket.toFixed(3) + '<br>' +
                              '<b>Потребление:</b> ' + bestRegion.avgConsumption.toFixed(3) + '<br>' +
                              '<b>Доступность:</b> ' + bestRegion.avgAccessibility.toFixed(3) + '<br>' +
                              '<b>Климат:</b> ' + bestRegion.avgClimate.toFixed(3) + '<br>' +
                              '<b>Мобильность:</b> ' + bestRegion.avgMobility.toFixed(3) + '<extra></extra>'
            };

            trace2 = {
                type: 'scatterpolar',
                r: [avgRussia.poad, avgRussia.market, avgRussia.consumption, avgRussia.accessibility, avgRussia.climate, avgRussia.mobility],
                theta: ['POAD', 'Рынок', 'Потребл.', 'Доступн.', 'Климат', 'Мобильн.'],
                fill: 'toself',
                name: 'Среднее по РФ',
                line: { color: 'rgba(128, 128, 128, 0.2)', width: 2 },
                fillcolor: 'rgba(128, 128, 128, 0.2)',
                hovertemplate: '<b>%{fullData.name}</b> (NDI: ' + avgRussia.ndi.toFixed(2) + ', ' + ndiWidgetData.length + ' НП)<br><br>' +
                              '<b>POAD:</b> ' + avgRussia.poad.toFixed(3) + '<br>' +
                              '<b>Рынок:</b> ' + avgRussia.market.toFixed(3) + '<br>' +
                              '<b>Потребление:</b> ' + avgRussia.consumption.toFixed(3) + '<br>' +
                              '<b>Доступность:</b> ' + avgRussia.accessibility.toFixed(3) + '<br>' +
                              '<b>Климат:</b> ' + avgRussia.climate.toFixed(3) + '<br>' +
                              '<b>Мобильность:</b> ' + avgRussia.mobility.toFixed(3) + '<extra></extra>'
            };
        } else {
            const regionName = ndiSelectedSettlement.region_name;
            const regionNPs = ndiWidgetData.filter(d => d.region_name === regionName);

            const avgRegion = {
                poad: regionNPs.reduce((s, d) => s + d.poad_score, 0) / regionNPs.length,
                market: regionNPs.reduce((s, d) => s + d.market_score, 0) / regionNPs.length,
                consumption: regionNPs.reduce((s, d) => s + d.consumption_score, 0) / regionNPs.length,
                accessibility: regionNPs.reduce((s, d) => s + d.accessibility_score, 0) / regionNPs.length,
                climate: regionNPs.reduce((s, d) => s + d.climate_score, 0) / regionNPs.length,
                mobility: regionNPs.reduce((s, d) => s + d.mobility_score, 0) / regionNPs.length,
                ndi: regionNPs.reduce((sum, d) => sum + d.ndi_10, 0) / regionNPs.length
            };

            const npColor = getNDIColorRgba(ndiSelectedSettlement.ndi_10, 0.5);

            trace1 = {
                type: 'scatterpolar',
                r: [ndiSelectedSettlement.poad_score, ndiSelectedSettlement.market_score, ndiSelectedSettlement.consumption_score,
                    ndiSelectedSettlement.accessibility_score, ndiSelectedSettlement.climate_score, ndiSelectedSettlement.mobility_score],
                theta: ['POAD', 'Рынок', 'Потребл.', 'Доступн.', 'Климат', 'Мобильн.'],
                fill: 'toself',
                name: ndiSelectedSettlement.settlement_name,
                line: { color: npColor, width: 2 },
                fillcolor: npColor,
                hovertemplate: '<b>%{fullData.name}</b> (NDI: ' + ndiSelectedSettlement.ndi_10.toFixed(2) + ', ранг #' + ndiSelectedSettlement.ndi_rank + ')<br><br>' +
                              '<b>POAD:</b> ' + ndiSelectedSettlement.poad_score.toFixed(3) + '<br>' +
                              '<b>Рынок:</b> ' + ndiSelectedSettlement.market_score.toFixed(3) + '<br>' +
                              '<b>Потребление:</b> ' + ndiSelectedSettlement.consumption_score.toFixed(3) + '<br>' +
                              '<b>Доступность:</b> ' + ndiSelectedSettlement.accessibility_score.toFixed(3) + '<br>' +
                              '<b>Климат:</b> ' + ndiSelectedSettlement.climate_score.toFixed(3) + '<br>' +
                              '<b>Мобильность:</b> ' + ndiSelectedSettlement.mobility_score.toFixed(3) + '<extra></extra>'
            };

            trace2 = {
                type: 'scatterpolar',
                r: [avgRegion.poad, avgRegion.market, avgRegion.consumption, avgRegion.accessibility, avgRegion.climate, avgRegion.mobility],
                theta: ['POAD', 'Рынок', 'Потребл.', 'Доступн.', 'Климат', 'Мобильн.'],
                fill: 'toself',
                name: `Среднее по региону: ${regionName}`,
                line: { color: 'rgba(128, 128, 128, 0.2)', width: 2 },
                fillcolor: 'rgba(128, 128, 128, 0.2)',
                hovertemplate: '<b>%{fullData.name}</b> (NDI: ' + avgRegion.ndi.toFixed(2) + ', ' + regionNPs.length + ' НП)<br><br>' +
                              '<b>POAD:</b> ' + avgRegion.poad.toFixed(3) + '<br>' +
                              '<b>Рынок:</b> ' + avgRegion.market.toFixed(3) + '<br>' +
                              '<b>Потребление:</b> ' + avgRegion.consumption.toFixed(3) + '<br>' +
                              '<b>Доступность:</b> ' + avgRegion.accessibility.toFixed(3) + '<br>' +
                              '<b>Климат:</b> ' + avgRegion.climate.toFixed(3) + '<br>' +
                              '<b>Мобильность:</b> ' + avgRegion.mobility.toFixed(3) + '<extra></extra>'
            };
        }
    }

    const layout = {
        polar: {
            radialaxis: { visible: false, range: [0, 1] },
            angularaxis: { gridcolor: '#ecf0f1' }
        },
        title: 'Сравнение компонентов',
        height: 450,
        plot_bgcolor: 'white',
        paper_bgcolor: 'white',
        font: { family: 'Segoe UI, Arial, sans-serif', size: 12, color: '#2c3e50' },
        showlegend: true,
        legend: { orientation: 'h', y: -0.15 }
    };

    Plotly.newPlot('radar', [trace1, trace2], layout, { responsive: true });
}

// ============================================================================
// ПОИСК И ФИЛЬТРАЦИЯ
// ============================================================================
function filterNDISettlements(searchTerm) {
    const select = document.getElementById('settlement-select');
    if (!select) return;

    const options = Array.from(select.options);
    let firstVisibleOption = null;

    options.forEach(option => {
        const text = option.textContent.toLowerCase();
        const isMatch = text.includes(searchTerm.toLowerCase());
        option.style.display = isMatch ? '' : 'none';
        if (!firstVisibleOption && option.value !== 'average' && isMatch) {
            firstVisibleOption = option;
        }
    });

    if (searchTerm.trim() !== '' && firstVisibleOption) {
        select.value = firstVisibleOption.value;
        const idx = parseInt(firstVisibleOption.value);
        ndiSelectedSettlement = ndiWidgetData[idx];
        renderNDIWaterfall(ndiSelectedSettlement, ndiCurrentCompareMode);
        renderNDIRadar(ndiCurrentCompareMode);
    }
}

function updateNDIResetButton() {
    const resetBtn = document.getElementById('reset-settlement');
    const select = document.getElementById('settlement-select');
    const searchInput = document.getElementById('settlement-search');
    if (!resetBtn || !select || !searchInput) return;

    if (select.selectedIndex !== 0 || searchInput.value !== '') {
        resetBtn.classList.add('active');
    } else {
        resetBtn.classList.remove('active');
    }
}

function resetNDISelection() {
    const select = document.getElementById('settlement-select');
    const searchInput = document.getElementById('settlement-search');
    if (!select || !searchInput) return;

    searchInput.value = '';
    filterNDISettlements('');
    select.selectedIndex = 0;
    ndiSelectedSettlement = 'average';
    renderNDIWaterfall(ndiSelectedSettlement, ndiCurrentCompareMode);
    renderNDIRadar(ndiCurrentCompareMode);
    updateNDIResetButton();
}

// ============================================================================
// EVENT LISTENERS
// ============================================================================
document.addEventListener('DOMContentLoaded', () => {
    const settlementSelect = document.getElementById('settlement-select');
    const settlementSearch = document.getElementById('settlement-search');
    const resetBtn = document.getElementById('reset-settlement');
    const compareMode = document.getElementById('compare-mode');
    const regionFilter = document.getElementById('region-filter');

    if (settlementSelect) {
        settlementSelect.addEventListener('change', (e) => {
            const value = e.target.value;
            ndiSelectedSettlement = value === 'average' ? 'average' : ndiWidgetData[parseInt(value)];
            renderNDIWaterfall(ndiSelectedSettlement, ndiCurrentCompareMode);
            renderNDIRadar(ndiCurrentCompareMode);
            updateNDIResetButton();
            updateResetAllButtons();

            // Синхронизация с таблицей
            const tableSettlementSelect = document.getElementById('table-settlement-select');
            if (tableSettlementSelect && tableSettlementSelect.value !== value) {
                tableSettlementSelect.value = value;
            }
        });
    }

    if (settlementSearch) {
        settlementSearch.addEventListener('input', (e) => {
            filterNDISettlements(e.target.value);
            updateNDIResetButton();
            updateResetAllButtons();

            // Синхронизация с таблицей
            const tableSearch = document.getElementById('table-search');
            if (tableSearch && tableSearch.value !== e.target.value) {
                tableSearch.value = e.target.value;
            }
        });
    }

    if (resetBtn) {
        resetBtn.addEventListener('click', resetNDISelection);
    }

    if (compareMode) {
        compareMode.addEventListener('change', (e) => {
            ndiCurrentCompareMode = e.target.value;
            renderNDIWaterfall(ndiSelectedSettlement, ndiCurrentCompareMode);
            renderNDIRadar(ndiCurrentCompareMode);
            updateResetAllButtons();

            // Режим сравнения больше не синхронизируется с таблицей (его там нет)
        });
    }

    if (regionFilter) {
        regionFilter.addEventListener('change', (e) => {
            ndiSelectedRegion = e.target.value;
            ndiSelectedSettlement = 'average'; // Сбрасываем выбор НП при смене региона
            populateNDIDropdown();
            renderNDIWaterfall(ndiSelectedSettlement, ndiCurrentCompareMode);
            renderNDIRadar(ndiCurrentCompareMode);
            updateResetAllButtons();

            // Синхронизация с таблицей
            const tableRegionFilter = document.getElementById('table-region-filter');
            if (tableRegionFilter && tableRegionFilter.value !== e.target.value) {
                tableRegionFilter.value = e.target.value;
                tableRegionFilter.dispatchEvent(new Event('change'));
            }
        });
    }

    // Синхронизация обратно от таблицы к виджету
    const tableSettlementSelect = document.getElementById('table-settlement-select');
    if (tableSettlementSelect) {
        tableSettlementSelect.addEventListener('change', (e) => {
            const value = e.target.value;
            if (settlementSelect && settlementSelect.value !== value) {
                settlementSelect.value = value;
                ndiSelectedSettlement = value === 'average' ? 'average' : ndiWidgetData[parseInt(value)];
                renderNDIWaterfall(ndiSelectedSettlement, ndiCurrentCompareMode);
                renderNDIRadar(ndiCurrentCompareMode);
                updateNDIResetButton();
                updateResetAllButtons();
            }
        });
    }

    // table-compare-mode больше не существует (убран из третьего ряда фильтров)

    const tableSearch = document.getElementById('table-search');
    if (tableSearch) {
        tableSearch.addEventListener('input', (e) => {
            if (settlementSearch && settlementSearch.value !== e.target.value) {
                settlementSearch.value = e.target.value;
                filterNDISettlements(e.target.value);
                updateNDIResetButton();
                updateResetAllButtons();
            }
        });
    }

    // Кнопка "Сбросить всё" - первый уровень фильтров
    const resetAllFilters = document.getElementById('reset-all-filters');
    if (resetAllFilters) {
        resetAllFilters.addEventListener('click', () => {
            // Сбрасываем все фильтры
            if (settlementSearch) settlementSearch.value = '';
            if (settlementSelect) {
                settlementSelect.selectedIndex = 0;
                filterNDISettlements('');
            }
            if (compareMode) compareMode.value = 'vs-russia';
            if (regionFilter) regionFilter.value = 'all';

            // Обновляем состояние
            ndiSelectedSettlement = 'average';
            ndiCurrentCompareMode = 'vs-russia';
            ndiSelectedRegion = 'all';

            // Перерисовываем виджет
            populateNDIDropdown();
            renderNDIWaterfall(ndiSelectedSettlement, ndiCurrentCompareMode);
            renderNDIRadar(ndiCurrentCompareMode);
            updateNDIResetButton();
            updateResetAllButtons();

            // Синхронизируем с таблицей
            const tableSearch = document.getElementById('table-search');
            const tableSettlementSelect = document.getElementById('table-settlement-select');
            const tableRegionFilter = document.getElementById('table-region-filter');
            const arcticFilter = document.getElementById('arctic-filter');

            if (tableSearch) tableSearch.value = '';
            if (tableSettlementSelect) tableSettlementSelect.selectedIndex = 0;
            if (tableRegionFilter) {
                tableRegionFilter.value = 'all';
                tableRegionFilter.dispatchEvent(new Event('change'));
            }
            if (arcticFilter) {
                arcticFilter.value = 'all';
                arcticFilter.dispatchEvent(new Event('change'));
            }
        });
    }

    // Кнопка "Сбросить всё" - третий уровень фильтров (таблица)
    const tableResetAllFilters = document.getElementById('table-reset-all-filters');
    if (tableResetAllFilters) {
        tableResetAllFilters.addEventListener('click', () => {
            // Сбрасываем все фильтры таблицы
            const tableSearch = document.getElementById('table-search');
            const tableSettlementSelect = document.getElementById('table-settlement-select');
            const tableRegionFilter = document.getElementById('table-region-filter');
            const arcticFilter = document.getElementById('arctic-filter');

            if (tableSearch) tableSearch.value = '';
            if (tableSettlementSelect) tableSettlementSelect.selectedIndex = 0;
            if (tableRegionFilter) {
                tableRegionFilter.value = 'all';
                tableRegionFilter.dispatchEvent(new Event('change'));
            }
            if (arcticFilter) {
                arcticFilter.value = 'all';
                arcticFilter.dispatchEvent(new Event('change'));
            }

            // Синхронизируем с виджетом
            if (settlementSearch) settlementSearch.value = '';
            if (settlementSelect) {
                settlementSelect.selectedIndex = 0;
                filterNDISettlements('');
            }
            if (compareMode) compareMode.value = 'vs-russia';
            if (regionFilter) regionFilter.value = 'all';

            // Обновляем состояние виджета
            ndiSelectedSettlement = 'average';
            ndiCurrentCompareMode = 'vs-russia';
            ndiSelectedRegion = 'all';

            // Перерисовываем виджет
            populateNDIDropdown();
            renderNDIWaterfall(ndiSelectedSettlement, ndiCurrentCompareMode);
            renderNDIRadar(ndiCurrentCompareMode);
            updateNDIResetButton();
            updateResetAllButtons();
        });
    }

    // Загрузка данных виджета
    loadNDIWidgetData();
});
