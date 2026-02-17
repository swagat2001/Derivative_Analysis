// ============================================

// Global State
let globalFiiDiiState = {
    activeTab: 'equity',
    activeParticipant: 'both',
    timePeriod: 'daily',
    selectedDays: 30,
    rawData: null,
    niftyData: null,
    derivativesData: null,
    chartInstance: null
};

// ============================================
// INITIALIZATION
// ============================================
function initEnhancedFiiDii() {
    console.log('[FII/DII] Initializing enhanced module...');
    loadFiiDiiEnhanced();
}

// ============================================
// DATA FETCHING
// ============================================
async function loadFiiDiiEnhanced() {
    console.log('[FII/DII Enhanced] loadFiiDiiEnhanced() called');
    const container = document.getElementById('fii-dii-container');
    const days = globalFiiDiiState.selectedDays;

    console.log('[FII/DII Enhanced] Selected days:', days);
    console.log('[FII/DII Enhanced] Container element:', container);

    if (!globalFiiDiiState.rawData) {
        container.innerHTML = '<div class="loading-spinner"><div class="spinner"></div></div>';
    }

    try {
        // Get currentDate from global scope or use empty string
        const dateParam = (typeof currentDate !== 'undefined' && currentDate && currentDate !== 'None') ? currentDate : '';
        const url = `/neev/api/fii-dii?end_date=${dateParam}&days=${days}`;

        console.log('[FII/DII Enhanced] Fetching from URL:', url);

        const response = await fetch(url);
        const data = await response.json();

        console.log('[FII/DII Enhanced] API Response:', data);
        console.log('[FII/DII Enhanced] Data success:', data.success);
        console.log('[FII/DII Enhanced] Data.data length:', data.data?.length);
        console.log('[FII/DII Enhanced] Derivatives keys:', Object.keys(data.derivatives || {}));

        if (!data.success || (!data.data?.length && !Object.keys(data.derivatives || {}).length)) {
            console.warn('[FII/DII Enhanced] No data available');
            container.innerHTML = '<div class="empty-state"><p>No FII/DII Data Available</p></div>';
            return;
        }

        globalFiiDiiState.rawData = data.data || [];
        globalFiiDiiState.niftyData = data.nifty50 || {};
        globalFiiDiiState.derivativesData = data.derivatives || {};

        console.log('[FII/DII Enhanced] State updated - rawData count:', globalFiiDiiState.rawData.length);
        console.log('[FII/DII Enhanced] State updated - niftyData keys:', Object.keys(globalFiiDiiState.niftyData).length);

        renderEnhancedDashboard();

    } catch (e) {
        console.error('[FII/DII Enhanced] Error loading data:', e);
        console.error('[FII/DII Enhanced] Error stack:', e.stack);
        container.innerHTML = '<div class="empty-state"><p>Error loading data. Check console for details.</p></div>';
    }
}

// ============================================
// UTILITY FUNCTIONS
// ============================================
function formatNumber(num) {
    if (num === null || num === undefined) return '0';
    return parseFloat(num).toLocaleString('en-IN', { maximumFractionDigits: 2 });
}

// ============================================
// TIME AGGREGATION
// ============================================
function aggregateDataByPeriod(data, period) {
    if (period === 'daily' || !data || !data.length) return data;

    const grouped = {};

    data.forEach(row => {
        const date = new Date(row.date);
        let key;

        if (period === 'weekly') {
            const weekStart = new Date(date);
            weekStart.setDate(date.getDate() - date.getDay());
            key = weekStart.toISOString().split('T')[0];
        } else if (period === 'monthly') {
            key = `${date.getFullYear()}-${String(date.getMonth() + 1).padStart(2, '0')}-01`;
        } else if (period === 'yearly') {
            key = `${date.getFullYear()}-01-01`;
        }

        if (!grouped[key]) {
            grouped[key] = {
                date: key,
                fii_buy_value: 0,
                fii_sell_value: 0,
                fii_net_value: 0,
                dii_buy_value: 0,
                dii_sell_value: 0,
                dii_net_value: 0,
                total_net_value: 0,
                count: 0
            };
        }

        grouped[key].fii_buy_value += row.fii_buy_value || 0;
        grouped[key].fii_sell_value += row.fii_sell_value || 0;
        grouped[key].fii_net_value += row.fii_net_value || 0;
        grouped[key].dii_buy_value += row.dii_buy_value || 0;
        grouped[key].dii_sell_value += row.dii_sell_value || 0;
        grouped[key].dii_net_value += row.dii_net_value || 0;
        grouped[key].total_net_value += row.total_net_value || 0;
        grouped[key].count++;
    });

    return Object.values(grouped).sort((a, b) => new Date(a.date) - new Date(b.date));
}

// ============================================
// MAIN RENDER FUNCTION
// ============================================
function renderEnhancedDashboard() {
    const container = document.getElementById('fii-dii-container');
    const { activeTab, activeParticipant, timePeriod } = globalFiiDiiState;

    if (activeTab === 'equity') {
        renderEquityView(container);
    } else {
        renderDerivativeView(container, activeTab);
    }
}

// ============================================
// EQUITY VIEW RENDERER
// ============================================
function renderEquityView(container) {
    console.log('[FII/DII Enhanced] renderEquityView() called');
    const { activeParticipant, timePeriod, rawData, niftyData } = globalFiiDiiState;

    console.log('[FII/DII Enhanced] Active participant:', activeParticipant);
    console.log('[FII/DII Enhanced] Time period:', timePeriod);
    console.log('[FII/DII Enhanced] Raw data count:', rawData?.length);

    // Aggregate data based on time period
    const aggregatedData = aggregateDataByPeriod(rawData, timePeriod);
    console.log('[FII/DII Enhanced] Aggregated data count:', aggregatedData?.length);

    // Check if we have sufficient data
    if (!aggregatedData || aggregatedData.length === 0) {
        container.innerHTML = `
            <div class="empty-state">
                <h3>📊 No FII/DII Data</h3>
                <p>No cash market data available for the selected period.</p>
                <p style="color: #6b7280; margin-top: 12px;">Please check if the fii_dii_activity table has been populated.</p>
            </div>
        `;
        return;
    }

    if (aggregatedData.length < 5) {
        console.warn('[FII/DII Enhanced] Insufficient data - only', aggregatedData.length, 'days available');
    }

    // Determine which columns to show
    let tableHeaders = '';
    let showFii = activeParticipant === 'fii' || activeParticipant === 'both';
    let showDii = activeParticipant === 'dii' || activeParticipant === 'both';

    if (activeParticipant === 'fii') {
        tableHeaders = `
            <th class="sortable" onclick="sortTable(0)">Date <span class="sort-icon">↕</span></th>
            <th class="sortable" onclick="sortTable(1)">FII Buy (Cr) <span class="sort-icon">↕</span></th>
            <th class="sortable" onclick="sortTable(2)">FII Sell (Cr) <span class="sort-icon">↕</span></th>
            <th class="sortable" onclick="sortTable(3)">FII Net (Cr) <span class="sort-icon">↕</span></th>
        `;
    } else if (activeParticipant === 'dii') {
        tableHeaders = `
            <th class="sortable" onclick="sortTable(0)">Date <span class="sort-icon">↕</span></th>
            <th class="sortable" onclick="sortTable(1)">DII Buy (Cr) <span class="sort-icon">↕</span></th>
            <th class="sortable" onclick="sortTable(2)">DII Sell (Cr) <span class="sort-icon">↕</span></th>
            <th class="sortable" onclick="sortTable(3)">DII Net (Cr) <span class="sort-icon">↕</span></th>
        `;
    } else {
        tableHeaders = `
            <th class="sortable" onclick="sortTable(0)">Date <span class="sort-icon">↕</span></th>
            <th class="sortable" onclick="sortTable(1)">FII Buy (Cr) <span class="sort-icon">↕</span></th>
            <th class="sortable" onclick="sortTable(2)">FII Sell (Cr) <span class="sort-icon">↕</span></th>
            <th class="sortable" onclick="sortTable(3)">FII Net (Cr) <span class="sort-icon">↕</span></th>
            <th class="sortable" onclick="sortTable(4)">DII Buy (Cr) <span class="sort-icon">↕</span></th>
            <th class="sortable" onclick="sortTable(5)">DII Sell (Cr) <span class="sort-icon">↕</span></th>
            <th class="sortable" onclick="sortTable(6)">DII Net (Cr) <span class="sort-icon">↕</span></th>
        `;
    }

    // Generate table rows
    let tableRows = aggregatedData.slice().reverse().map(row => {
        if (activeParticipant === 'fii') {
            return `
                <tr>
                    <td>${row.date}</td>
                    <td>${formatNumber(row.fii_buy_value)}</td>
                    <td>${formatNumber(row.fii_sell_value)}</td>
                    <td class="${row.fii_net_value >= 0 ? 'positive' : 'negative'}" style="font-weight:700">${formatNumber(row.fii_net_value)}</td>
                </tr>
            `;
        } else if (activeParticipant === 'dii') {
            return `
                <tr>
                    <td>${row.date}</td>
                    <td>${formatNumber(row.dii_buy_value)}</td>
                    <td>${formatNumber(row.dii_sell_value)}</td>
                    <td class="${row.dii_net_value >= 0 ? 'positive' : 'negative'}" style="font-weight:700">${formatNumber(row.dii_net_value)}</td>
                </tr>
            `;
        } else {
            return `
                <tr>
                    <td>${row.date}</td>
                    <td>${formatNumber(row.fii_buy_value)}</td>
                    <td>${formatNumber(row.fii_sell_value)}</td>
                    <td class="${row.fii_net_value >= 0 ? 'positive' : 'negative'}" style="font-weight:700">${formatNumber(row.fii_net_value)}</td>
                    <td>${formatNumber(row.dii_buy_value)}</td>
                    <td>${formatNumber(row.dii_sell_value)}</td>
                    <td class="${row.dii_net_value >= 0 ? 'positive' : 'negative'}" style="font-weight:700">${formatNumber(row.dii_net_value)}</td>
                </tr>
            `;
        }
    }).join('');

    container.innerHTML = `
        <div class="chart-card" style="margin-bottom: 16px;">
            <div class="chart-title">Net Activity (${timePeriod.charAt(0).toUpperCase() + timePeriod.slice(1)})</div>
            <div id="dynamicMainChart" style="height: 400px; width: 100%;"></div>
        </div>

        <div class="screener-card">
            <div class="card-header">
                <span class="card-title">Detailed Activity Log (${activeParticipant.toUpperCase()})</span>
            </div>
            <table class="card-table" id="fiiDiiDataTable">
                <thead>
                    <tr>${tableHeaders}</tr>
                </thead>
                <tbody>${tableRows}</tbody>
            </table>
        </div>
    `;

    // Render chart
    console.log('[FII/DII Enhanced] About to render chart with', aggregatedData?.length, 'data points');
    renderDualAxisChart(aggregatedData, showFii, showDii);
}

// ============================================
// DUAL-AXIS CHART (NET VALUE + NIFTY PRICE)
// ============================================
function renderDualAxisChart(data, showFii, showDii) {
    console.log('[FII/DII Enhanced] renderDualAxisChart() called');
    console.log('[FII/DII Enhanced] Data points:', data?.length);
    console.log('[FII/DII Enhanced] showFii:', showFii, 'showDii:', showDii);

    const { niftyData } = globalFiiDiiState;
    console.log('[FII/DII Enhanced] Nifty data available:', Object.keys(niftyData || {}).length, 'dates');

    const labels = data.map(d => {
        const date = new Date(d.date);
        return `${date.getDate()}/${date.getMonth() + 1}`;
    });
    console.log('[FII/DII Enhanced] Chart labels:', labels.length);

    const fiiNetData = data.map(d => d.fii_net_value);
    const diiNetData = data.map(d => d.dii_net_value);
    const niftyPrices = data.map(d => niftyData[d.date] || null);

    const series = [];

    if (showFii) {
        series.push({
            name: 'FII Net',
            type: 'column',
            data: fiiNetData.map(v => ({ y: v, color: v >= 0 ? '#22c55e' : '#ef4444' })),
            yAxis: 0
        });
    }

    if (showDii) {
        series.push({
            name: 'DII Net',
            type: 'column',
            data: diiNetData.map(v => ({ y: v, color: '#f97316' })),
            yAxis: 0
        });
    }

    series.push({
        name: 'Nifty 50',
        type: 'line',
        data: niftyPrices,
        yAxis: 1,
        color: '#3b82f6',
        marker: { radius: 3 }
    });

    console.log('[FII/DII Enhanced] Series configuration:', series.length, 'series');
    console.log('[FII/DII Enhanced] Highcharts available:', typeof Highcharts !== 'undefined');

    if (typeof Highcharts === 'undefined') {
        console.error('[FII/DII Enhanced] Highcharts library not loaded!');
        document.getElementById('dynamicMainChart').innerHTML = '<div style="padding:20px;text-align:center;color:#ef4444;">Highcharts library not loaded. Please refresh the page.</div>';
        return;
    }

    Highcharts.chart('dynamicMainChart', {
        chart: {
            backgroundColor: 'transparent',
            style: { fontFamily: 'Inter, system-ui, sans-serif' },
            height: data.length === 1 ? 350 : 400
        },
        title: { text: null },
        xAxis: {
            categories: labels,
            crosshair: true,
            min: 0,
            max: data.length === 1 ? 0 : undefined  // Fix single point rendering
        },
        plotOptions: {
            column: {
                pointWidth: data.length === 1 ? 60 : undefined,  // Wider bars for single point
                maxPointWidth: 80
            }
        },
        yAxis: [
            {
                title: { text: 'Net Value (₹ Cr)' },
                labels: { style: { color: '#64748b' } }
            },
            {
                title: { text: 'Nifty 50 Price' },
                labels: { style: { color: '#3b82f6' } },
                opposite: true
            }
        ],
        tooltip: { shared: true },
        credits: { enabled: false },
        series: series
    });
}

// ============================================
// DERIVATIVES VIEW RENDERER
// ============================================
function renderDerivativeView(container, category) {
    console.log('[FII/DII Enhanced] renderDerivativeView() called for category:', category);
    const { derivativesData, activeParticipant, timePeriod } = globalFiiDiiState;

    console.log('[FII/DII Enhanced] Derivatives data keys:', Object.keys(derivativesData || {}).length);
    console.log('[FII/DII Enhanced] Active participant:', activeParticipant);

    // Map category names
    const categoryMap = {
        'index_futures': 'index_futures',
        'index_options': 'index_options',
        'stock_futures': 'stock_futures',
        'stock_options': 'stock_options'
    };

    const mappedCategory = categoryMap[category] || category;
    const participant = activeParticipant === 'both' ? 'FII' : activeParticipant.toUpperCase();

    console.log('[FII/DII Enhanced] Mapped category:', mappedCategory, 'Participant:', participant);

    // Extract and aggregate data for this category and participant
    const aggregatedData = aggregateDerivativesForChart(derivativesData, mappedCategory, participant);
    console.log('[FII/DII Enhanced] Aggregated derivatives data points:', aggregatedData.length);

    if (aggregatedData.length === 0) {
        container.innerHTML = `
            <div class="empty-state">
                <h3>📊 No ${category.replace('_', ' ').toUpperCase()} Data</h3>
                <p>No derivatives data available for ${participant} in this category.</p>
                <p style="color: #6b7280; margin-top: 12px;">Try selecting a different participant or check if derivatives data is populated.</p>
            </div>
        `;
        return;
    }

    const isOptions = (mappedCategory === 'index_options' || mappedCategory === 'stock_options');
    const isFutures = (mappedCategory === 'index_futures' || mappedCategory === 'stock_futures');

    let tableHeaders = '';
    let tableRows = '';

    if (isOptions) {
        tableHeaders = `
            <th>Date</th>
            <th>Call Buy OI</th>
            <th>Call Sell OI</th>
            <th>Call Net OI</th>
            <th>Put Buy OI</th>
            <th>Put Sell OI</th>
            <th>Put Net OI</th>
        `;
        tableRows = renderOptionsTableRows(derivativesData, mappedCategory, participant);
    } else if (isFutures) {
        // Futures: Specific headers for contracts
        tableHeaders = `
            <th>Date</th>
            <th>Buy OI (Contracts)</th>
            <th>Sell OI (Contracts)</th>
            <th>Net OI (Contracts)</th>
        `;
        tableRows = renderDerivativeTableRows(derivativesData, mappedCategory, participant);
    } else {
        tableHeaders = `
            <th>Date</th>
            <th>Buy Value (Cr)</th>
            <th>Sell Value (Cr)</th>
            <th>Net Value (Cr)</th>
            <th>OI Contracts</th>
            <th>OI Long</th>
            <th>OI Short</th>
        `;
        tableRows = renderDerivativeTableRows(derivativesData, mappedCategory, participant);
    }

    container.innerHTML = `
        <div class="chart-card" style="margin-bottom: 16px;">
            <div class="chart-title">${category.replace(/_/g, ' ').toUpperCase()} - ${participant}</div>
            <div id="dynamicMainChart" style="height: 400px; width: 100%;"></div>
        </div>

        <div class="screener-card">
            <div class="card-header">
                <span class="card-title">Activity Details</span>
            </div>
            <table class="card-table sortable-table">
                <thead>
                    <tr>${tableHeaders}</tr>
                </thead>
                <tbody>
                    ${tableRows}
                </tbody>
            </table>
        </div>
    `;
    if (window.initTableSorting) window.initTableSorting();

    // Render appropriate chart
    if (isOptions) {
        renderOptionsChart(derivativesData, mappedCategory, participant);
    } else if (isFutures) {
        renderFuturesChart(derivativesData, mappedCategory, participant);
    } else {
        renderDerivativesChart(aggregatedData, category, participant);
    }
}

function aggregateDerivativesForChart(derivData, category, participant) {
    console.log('[FII/DII Enhanced] aggregateDerivativesForChart() called');

    if (!derivData || Object.keys(derivData).length === 0) {
        console.warn('[FII/DII Enhanced] No derivatives data to aggregate');
        return [];
    }

    const result = [];
    const dates = Object.keys(derivData).sort();

    console.log('[FII/DII Enhanced] Processing', dates.length, 'dates for', category, participant);

    dates.forEach(date => {
        const items = derivData[date].filter(x =>
            x.category === category && x.participant_type === participant
        );

        if (items.length > 0) {
            // Sum all items for this date/category/participant
            const dayTotal = items.reduce((acc, item) => ({
                buy_value: acc.buy_value + (item.buy_value || 0),
                sell_value: acc.sell_value + (item.sell_value || 0),
                net_value: acc.net_value + (item.net_value || 0),
                oi_contracts: acc.oi_contracts + (item.oi_contracts || 0)
            }), { buy_value: 0, sell_value: 0, net_value: 0, oi_contracts: 0 });

            result.push({
                date: date,
                ...dayTotal
            });
        }
    });

    console.log('[FII/DII Enhanced] Aggregated to', result.length, 'data points');
    return result;
}

// NEW: Specific Chart for Futures (Long vs Short)
function renderFuturesChart(derivData, category, participant) {
    console.log('[FII/DII Enhanced] renderFuturesChart called');
    const dates = Object.keys(derivData).sort().slice(-30); // Last 30 days
    const labels = dates.map(d => {
        const date = new Date(d);
        return `${date.getDate()}/${date.getMonth() + 1}`;
    });

    const longData = [];
    const shortData = [];
    const netData = [];
    const niftyPrices = [];

    dates.forEach(d => {
        const item = derivData[d].find(x => x.category === category && x.participant_type === participant);
        const l = item ? (item.oi_long || 0) : 0;
        const s = item ? (item.oi_short || 0) : 0;
        longData.push(l);
        shortData.push(s);
        netData.push(l - s);

        // Nifty Price
        niftyPrices.push(globalFiiDiiState.niftyData[d] || null);
    });

    const hasNiftyData = niftyPrices.some(v => v !== null);

    if (typeof Highcharts === 'undefined') return;

    Highcharts.chart('dynamicMainChart', {
        chart: { backgroundColor: 'transparent', style: { fontFamily: 'Inter, system-ui, sans-serif' }, height: 400 },
        title: { text: null },
        xAxis: { categories: labels, crosshair: true },
        yAxis: [
            { title: { text: 'Contracts' }, labels: { style: { color: '#64748b' } } },
            { title: { text: 'Nifty 50' }, opposite: true, visible: hasNiftyData }
        ],
        tooltip: { shared: true },
        credits: { enabled: false },
        series: [
            { name: 'Long OI', type: 'column', data: longData, color: '#22c55e', borderWidth: 0 },
            { name: 'Short OI', type: 'column', data: shortData, color: '#ef4444', borderWidth: 0 },
            { name: 'Net OI', type: 'line', data: netData, color: '#f59e0b', marker: { radius: 3 }, yAxis: 0 },
            { name: 'Nifty 50', type: 'line', data: niftyPrices, yAxis: 1, color: '#3b82f6', visible: hasNiftyData, dashStyle: 'ShortDot', marker: { enabled: false } }
        ]
    });
}

// NEW: Specific Chart for Options (Call vs Put Net OI)
function renderOptionsChart(derivData, category, participant) {
    console.log('[FII/DII Enhanced] renderOptionsChart called');
    const dates = Object.keys(derivData).sort().slice(-30);
    const labels = dates.map(d => {
        const date = new Date(d);
        return `${date.getDate()}/${date.getMonth() + 1}`;
    });

    const type = category.split('_')[0]; // index or stock
    const callCat = `${type}_call_options`;
    const putCat = `${type}_put_options`;

    const callNetData = [];
    const putNetData = [];
    const netPremiumData = [];
    const niftyPrices = [];

    dates.forEach(d => {
        const items = derivData[d];
        const callItem = items.find(x => x.category === callCat && x.participant_type === participant);
        const putItem = items.find(x => x.category === putCat && x.participant_type === participant);
        const aggItem = items.find(x => x.category === category && x.participant_type === participant);

        callNetData.push(callItem ? (callItem.oi_long - callItem.oi_short) : 0);
        putNetData.push(putItem ? (putItem.oi_long - putItem.oi_short) : 0);
        netPremiumData.push(aggItem ? aggItem.net_value : 0);

        niftyPrices.push(globalFiiDiiState.niftyData[d] || null);
    });

    const hasNiftyData = niftyPrices.some(v => v !== null);

    if (typeof Highcharts === 'undefined') return;

    Highcharts.chart('dynamicMainChart', {
        chart: { backgroundColor: 'transparent', style: { fontFamily: 'Inter, system-ui, sans-serif' }, height: 400 },
        title: { text: null },
        xAxis: { categories: labels, crosshair: true },
        yAxis: [
            { title: { text: 'Net OI (Contracts)' }, labels: { style: { color: '#64748b' } } },
            { title: { text: 'Nifty 50' }, opposite: true, visible: hasNiftyData }
        ],
        tooltip: { shared: true },
        credits: { enabled: false },
        series: [
            { name: 'Call Net OI', type: 'column', data: callNetData, color: '#22c55e', yAxis: 0 },
            { name: 'Put Net OI', type: 'column', data: putNetData, color: '#ef4444', yAxis: 0 },
            { name: 'Nifty 50', type: 'line', data: niftyPrices, yAxis: 1, color: '#3b82f6', visible: hasNiftyData, marker: { enabled: false } }
        ]
    });
}

function renderOptionsTableRows(derivData, baseCategory, participant) {
    const dates = Object.keys(derivData).sort().reverse();
    const type = baseCategory.includes('index') ? 'index' : 'stock';
    const callCat = `${type}_call_options`;
    const putCat = `${type}_put_options`;

    let rows = '';

    dates.forEach(date => {
        const dataArr = derivData[date];
        const callItem = dataArr.find(x => x.category === callCat && x.participant_type === participant);
        const putItem = dataArr.find(x => x.category === putCat && x.participant_type === participant);

        if (callItem || putItem) {
            const cBuy = callItem ? callItem.oi_long : 0;
            const cSell = callItem ? callItem.oi_short : 0;
            const cNet = cBuy - cSell;

            const pBuy = putItem ? putItem.oi_long : 0;
            const pSell = putItem ? putItem.oi_short : 0;
            const pNet = pBuy - pSell;

            rows += `
                <tr>
                    <td>${date}</td>
                    <td>${formatNumber(cBuy)}</td>
                    <td>${formatNumber(cSell)}</td>
                    <td class="${cNet >= 0 ? 'positive' : 'negative'}" style="font-weight:700">${formatNumber(cNet)}</td>
                    <td>${formatNumber(pBuy)}</td>
                    <td>${formatNumber(pSell)}</td>
                    <td class="${pNet >= 0 ? 'positive' : 'negative'}" style="font-weight:700">${formatNumber(pNet)}</td>
                </tr>
            `;
        }
    });

    return rows || `<tr><td colspan="7" style="text-align:center;">No Options data available</td></tr>`;
}

function renderDerivativeTableRows(derivData, category, participant) {
    const dates = Object.keys(derivData).sort().reverse();
    let rows = '';

    const isFutures = (category === 'index_futures' || category === 'stock_futures');

    dates.forEach(date => {
        const items = derivData[date].filter(x => x.category === category && x.participant_type === participant);
        items.forEach(item => {
            if (isFutures) {
                // Futures: Contracts and Position
                const long_oi = item.oi_long || 0;
                const short_oi = item.oi_short || 0;
                const net_oi = long_oi - short_oi;
                const position = net_oi > 0 ? 'Bullish' : net_oi < 0 ? 'Bearish' : 'Neutral';
                const posClass = net_oi > 0 ? 'positive' : net_oi < 0 ? 'negative' : '';

                rows += `
                    <tr>
                        <td>${date}</td>
                        <td>${formatNumber(long_oi)}</td>
                        <td>${formatNumber(short_oi)}</td>
                        <td class="${posClass}" style="font-weight:700">
                            ${net_oi >= 0 ? '+' : ''}${formatNumber(net_oi)}
                        </td>
                    </tr>
                `;
            } else {
                // Options list: Full 7 columns
                rows += `
                    <tr>
                        <td>${date}</td>
                        <td>${formatNumber(item.buy_value)}</td>
                        <td>${formatNumber(item.sell_value)}</td>
                        <td class="${item.net_value >= 0 ? 'positive' : 'negative'}" style="font-weight:700">${formatNumber(item.net_value)}</td>
                        <td>${formatNumber(item.oi_contracts)}</td>
                        <td>${formatNumber(item.oi_long || 0)}</td>
                        <td>${formatNumber(item.oi_short || 0)}</td>
                    </tr>
                `;
            }
        });
    });

    const colspan = isFutures ? "4" : "7";
    return rows || `<tr><td colspan="${colspan}" style="text-align:center;">No data available</td></tr>`;
}

// ============================================
// TAB SWITCHING
// ============================================
function switchFiiTab(tab) {
    globalFiiDiiState.activeTab = tab;

    document.querySelectorAll('.fii-tab-btn').forEach(btn => btn.classList.remove('active'));
    document.getElementById(`btn-${tab}`).classList.add('active');

    renderEnhancedDashboard();
}

function updateParticipant() {
    globalFiiDiiState.activeParticipant = document.getElementById('fiiParticipant').value;
    renderEnhancedDashboard();
}

function updateTimePeriod() {
    globalFiiDiiState.timePeriod = document.getElementById('fiiTimePeriod').value;
    renderEnhancedDashboard();
}

// ============================================
// TABLE SORTING
// ============================================
let currentSortColumn = -1;
let currentSortDirection = 'asc';

function sortTable(columnIndex) {
    const table = document.getElementById('fiiDiiDataTable');
    const tbody = table.querySelector('tbody');
    const rows = Array.from(tbody.querySelectorAll('tr'));

    if (currentSortColumn === columnIndex) {
        currentSortDirection = currentSortDirection === 'asc' ? 'desc' : 'asc';
    } else {
        currentSortColumn = columnIndex;
        currentSortDirection = 'asc';
    }

    rows.sort((a, b) => {
        const aValue = a.querySelectorAll('td')[columnIndex].textContent.replace(/[^0-9.-]/g, '');
        const bValue = b.querySelectorAll('td')[columnIndex].textContent.replace(/[^0-9.-]/g, '');

        const aNum = parseFloat(aValue) || aValue;
        const bNum = parseFloat(bValue) || bValue;

        if (currentSortDirection === 'asc') {
            return aNum > bNum ? 1 : -1;
        } else {
            return aNum < bNum ? 1 : -1;
        }
    });

    rows.forEach(row => tbody.appendChild(row));

    // Update sort icons
    table.querySelectorAll('.sort-icon').forEach((icon, idx) => {
        if (idx === columnIndex) {
            icon.textContent = currentSortDirection === 'asc' ? '↑' : '↓';
        } else {
            icon.textContent = '↕';
        }
    });
}
