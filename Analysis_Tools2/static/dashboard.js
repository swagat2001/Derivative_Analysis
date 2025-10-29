let totalTable, otmTable, itmTable;
let currentChart = null;
let isLightweightChart = false;


const dateSelect = document.getElementById('currentDate');
availableDates.forEach(d => {
    const opt = document.createElement('option');
    opt.value = d;
    opt.textContent = d;
    dateSelect.appendChild(opt);
});


function formatNumber(val) {
    if (val === 0) return '0';
    const num = parseFloat(val);
    if (isNaN(num)) return val;
    if (Math.abs(num) >= 1e9) return (num/1e9).toFixed(2) + 'B';
    if (Math.abs(num) >= 1e6) return (num/1e6).toFixed(2) + 'M';
    if (Math.abs(num) >= 1e3) return (num/1e3).toFixed(2) + 'K';
    return num.toFixed(2);
}


async function loadData() {
    const currDate = document.getElementById('currentDate').value;
    if (!currDate) { 
        alert('Select date'); 
        return; 
    }
    
    document.getElementById('total-tab').innerHTML = '<div class="loading">Loading...</div>';
    document.getElementById('otm-tab').innerHTML = '<div class="loading">Loading...</div>';
    document.getElementById('itm-tab').innerHTML = '<div class="loading">Loading...</div>';
    
    try {
        const response = await fetch(`/get_data?date=${currDate}`);
        const data = await response.json();
        
        document.getElementById('dateDisplay').textContent = `📅 ${data.prev_date} → ${data.curr_date}`;
        
        // Destroy existing tables
        if (totalTable) totalTable.destroy();
        if (otmTable) otmTable.destroy();
        if (itmTable) itmTable.destroy();
        
        // Generate tables
        document.getElementById('total-tab').innerHTML = generateTableHTML(data.total || [], 'total');
        document.getElementById('otm-tab').innerHTML = generateTableHTML(data.otm, 'otm');
        document.getElementById('itm-tab').innerHTML = generateTableHTML(data.itm, 'itm');
        
        // Reinitialize DataTables with proper configuration
        // Initialize all tables immediately to prevent first-time sinking
        totalTable = $('#table_total').DataTable({
            pageLength: 50,
            order: [[0, 'asc']],
            scrollX: true,
            scrollY: 'calc(100vh - 400px)',
            scrollCollapse: true,
            fixedHeader: true,
            fixedColumns: { left: 1 },
            autoWidth: false,
            deferRender: true
        });
        
        otmTable = $('#table_otm').DataTable({
            pageLength: 50,
            order: [[0, 'asc']],
            scrollX: true,
            scrollY: 'calc(100vh - 400px)',
            scrollCollapse: true,
            fixedHeader: true,
            fixedColumns: { left: 1 },
            autoWidth: false,
            deferRender: true
        });
        
        itmTable = $('#table_itm').DataTable({
            pageLength: 50,
            order: [[0, 'asc']],
            scrollX: true,
            scrollY: 'calc(100vh - 400px)',
            scrollCollapse: true,
            fixedHeader: true,
            fixedColumns: { left: 1 },
            autoWidth: false,
            deferRender: true
        });
        
        // Force initial column adjustment for all tables after initialization
        setTimeout(() => {
            totalTable.columns.adjust().draw();
            otmTable.columns.adjust().draw();
            itmTable.columns.adjust().draw();
        }, 100);
    } catch (error) {
        document.getElementById('total-tab').innerHTML = '<div class="loading">Error loading data</div>';
        document.getElementById('otm-tab').innerHTML = '<div class="loading">Error loading data</div>';
        document.getElementById('itm-tab').innerHTML = '<div class="loading">Error loading data</div>';
    }
}


function generateTableHTML(data, id) {
    if (!data || data.length === 0) return '<div class="loading">No data available</div>';
    
    let html = `<table id="table_${id}" class="display"><thead><tr>`;
    html += '<th class="stock-header">Stock</th>';
    html += '<th class="call-header">Call Δ+ Strike</th>';
    html += '<th class="call-header">Call Δ+ %</th>';
    html += '<th class="call-header">Call Δ- Strike</th>';
    html += '<th class="call-header">Call Δ- %</th>';
    html += '<th class="call-header">Call Vega+ Strike</th>';
    html += '<th class="call-header">Call Vega+ %</th>';
    html += '<th class="call-header">Call Vega- Strike</th>';
    html += '<th class="call-header">Call Vega- %</th>';
    html += '<th class="call-header">Call ΔTV</th>';
    html += '<th class="call-header">Call ΔMoney</th>';
    html += '<th class="closing-header">Close</th>';
    html += '<th class="rsi-header">RSI</th>'; // ADDED RSI COLUMN HEADER
    html += '<th class="put-header">Put Δ+ Strike</th>';
    html += '<th class="put-header">Put Δ+ %</th>';
    html += '<th class="put-header">Put Δ- Strike</th>';
    html += '<th class="put-header">Put Δ- %</th>';
    html += '<th class="put-header">Put Vega+ Strike</th>';
    html += '<th class="put-header">Put Vega+ %</th>';
    html += '<th class="put-header">Put Vega- Strike</th>';
    html += '<th class="put-header">Put Vega- %</th>';
    html += '<th class="put-header">Put ΔTV</th>';
    html += '<th class="put-header">Put ΔMoney</th>';
    html += '</tr></thead><tbody>';
    
    data.forEach(row => {
        html += '<tr>';
        html += `<td class="stock-name" style="cursor: pointer;" onclick="window.location.href='/stock/${row.stock}'">${row.stock}</td>`;
        html += `<td class="strike-value">${row.call_delta_pos_strike}</td>`;
        html += `<td class="percentage ${parseFloat(row.call_delta_pos_pct)>=0?'positive':'negative'}">${row.call_delta_pos_pct}%</td>`;
        html += `<td class="strike-value">${row.call_delta_neg_strike}</td>`;
        html += `<td class="percentage ${parseFloat(row.call_delta_neg_pct)>=0?'positive':'negative'}">${row.call_delta_neg_pct}%</td>`;
        html += `<td class="strike-value">${row.call_vega_pos_strike}</td>`;
        const callVegaPosClick = row.call_vega_pos_strike !== 'N/A' ? `showHistoricalChart('${row.stock}', 'call', 'vega', '${row.call_vega_pos_strike}')` : `alert('No strike data available')`;
        html += `<td class="percentage money-cell ${parseFloat(row.call_vega_pos_pct)>=0?'positive':'negative'}" onclick="${callVegaPosClick}">${row.call_vega_pos_pct}%</td>`;
        html += `<td class="strike-value">${row.call_vega_neg_strike}</td>`;
        const callVegaNegClick = row.call_vega_neg_strike !== 'N/A' ? `showHistoricalChart('${row.stock}', 'call', 'vega', '${row.call_vega_neg_strike}')` : `alert('No strike data available')`;
        html += `<td class="percentage money-cell ${parseFloat(row.call_vega_neg_pct)>=0?'positive':'negative'}" onclick="${callVegaNegClick}">${row.call_vega_neg_pct}%</td>`;
        html += `<td class="${parseFloat(row.call_total_tradval)>=0?'positive':'negative'}">${formatNumber(row.call_total_tradval)}</td>`;
        html += `<td class="money-cell ${parseFloat(row.call_total_money)>=0?'positive':'negative'}" data-ticker="${row.stock}" data-type="call" onclick="showHistoricalChart('${row.stock}', 'call', 'money', null)">${formatNumber(row.call_total_money)}</td>`;
        html += `<td class="closing-price">${row.closing_price.toFixed(2)}</td>`;
        
        // ADDED RSI CELL
        const rsiValue = (row.rsi !== null && row.rsi !== undefined) ? row.rsi.toFixed(2) : 'N/A';
        const rsiClass = (row.rsi !== null && row.rsi !== undefined) ? 
            (row.rsi > 70 ? 'rsi-overbought' : (row.rsi < 30 ? 'rsi-oversold' : 'rsi-neutral')) : '';
        html += `<td class="rsi-value ${rsiClass}">${rsiValue}</td>`;
        
        html += `<td class="strike-value">${row.put_delta_pos_strike}</td>`;
        html += `<td class="percentage ${parseFloat(row.put_delta_pos_pct)>=0?'positive':'negative'}">${row.put_delta_pos_pct}%</td>`;
        html += `<td class="strike-value">${row.put_delta_neg_strike}</td>`;
        html += `<td class="percentage ${parseFloat(row.put_delta_neg_pct)>=0?'positive':'negative'}">${row.put_delta_neg_pct}%</td>`;
        html += `<td class="strike-value">${row.put_vega_pos_strike}</td>`;
        const putVegaPosClick = row.put_vega_pos_strike !== 'N/A' ? `showHistoricalChart('${row.stock}', 'put', 'vega', '${row.put_vega_pos_strike}')` : `alert('No strike data available')`;
        html += `<td class="percentage money-cell ${parseFloat(row.put_vega_pos_pct)>=0?'positive':'negative'}" onclick="${putVegaPosClick}">${row.put_vega_pos_pct}%</td>`;
        html += `<td class="strike-value">${row.put_vega_neg_strike}</td>`;
        const putVegaNegClick = row.put_vega_neg_strike !== 'N/A' ? `showHistoricalChart('${row.stock}', 'put', 'vega', '${row.put_vega_neg_strike}')` : `alert('No strike data available')`;
        html += `<td class="percentage money-cell ${parseFloat(row.put_vega_neg_pct)>=0?'positive':'negative'}" onclick="${putVegaNegClick}">${row.put_vega_neg_pct}%</td>`;
        html += `<td class="${parseFloat(row.put_total_tradval)>=0?'positive':'negative'}">${formatNumber(row.put_total_tradval)}</td>`;
        html += `<td class="money-cell ${parseFloat(row.put_total_money)>=0?'positive':'negative'}" data-ticker="${row.stock}" data-type="put" onclick="showHistoricalChart('${row.stock}', 'put', 'money', null)">${formatNumber(row.put_total_money)}</td>`;
        html += '</tr>';
    });
    
    return html + '</tbody></table>';
}


function showTab(tab) {
    document.querySelectorAll('.tab').forEach(t => t.classList.remove('active'));
    document.querySelectorAll('.tab-content').forEach(t => t.classList.remove('active'));
    event.target.classList.add('active');
    document.getElementById(tab + '-tab').classList.add('active');

    // Get current search value from any existing table
    let globalSearch = '';
    if (totalTable) {
        globalSearch = totalTable.search();
    } else if (otmTable) {
        globalSearch = otmTable.search();
    } else if (itmTable) {
        globalSearch = itmTable.search();
    }

    // Wait for tab to be visible, then adjust and apply search
    requestAnimationFrame(() => {
        setTimeout(() => {
            if (tab === 'total' && totalTable) {
                // Apply search if exists
                if (globalSearch) {
                    totalTable.search(globalSearch);
                }
                totalTable.columns.adjust();
                totalTable.fixedHeader.adjust();
                totalTable.draw(false);
            } else if (tab === 'otm' && otmTable) {
                // Apply search from total table
                if (globalSearch) {
                    otmTable.search(globalSearch);
                }
                otmTable.columns.adjust();
                otmTable.fixedHeader.adjust();
                otmTable.draw(false);
            } else if (tab === 'itm' && itmTable) {
                // Apply search from total table
                if (globalSearch) {
                    itmTable.search(globalSearch);
                }
                itmTable.columns.adjust();
                itmTable.fixedHeader.adjust();
                itmTable.draw(false);
            }
        }, 10);
    });
}


async function showHistoricalChart(ticker, optionType, metric = 'money', strike = null) {
    const currDate = document.getElementById('currentDate').value;
    if (!currDate) return;
    
    try {
        let url = `/get_historical_data?ticker=${ticker}&date=${currDate}&type=${optionType}&metric=${metric}`;
        if (strike && strike !== 'N/A' && metric === 'vega') {
            url += `&strike=${strike}`;
        }
        
        const response = await fetch(url);
        const data = await response.json();
        
        if (data.error) {
            alert('Error loading historical data: ' + data.error);
            return;
        }
        
        // Show modal
        document.getElementById('chartModal').style.display = 'block';
        
        // Enhanced title with strike price
        let titleText = `${ticker} - ${optionType.toUpperCase()} Historical Data (40 Days)`;
        if (strike && strike !== 'N/A') {
            titleText += ` - Strike: ${strike}`;
        }
        titleText += metric === 'vega' ? ' - Vega Trend' : ' - Moneyness';
        document.getElementById('chartTitle').textContent = titleText;
        
        // Clear legend header
        const legendHeader = document.getElementById('chart-legend-header');
        if (legendHeader) {
            legendHeader.innerHTML = '';
        }
        
        // Destroy previous chart
        if (currentChart) {
            if (isLightweightChart) {
                currentChart.remove();
            }
            currentChart = null;
        }
        // Clean up previous RSI chart
        if (window.rsiChart) {
            window.rsiChart.remove();
            window.rsiChart = null;
        }
        
        // Clear the chart container and create proper structure for two panels
        const chartContainer = document.querySelector('.chart-container');
        const containerHeight = chartContainer.clientHeight;
        const containerWidth = chartContainer.clientWidth;
        
        // Calculate dynamic heights
        const mainChartHeight = Math.floor(containerHeight * 0.72);
        const rsiChartHeight = Math.floor(containerHeight * 0.28);
        
        chartContainer.innerHTML = `
            <div id="main_chart_wrapper" style="height: ${mainChartHeight}px; width: 100%; position: relative;">
                <div id="tradingview_chart" style="height: 100%; width: 100%;"></div>
            </div>
            <div id="rsi_chart_wrapper" style="height: ${rsiChartHeight}px; width: 100%; position: relative; border-top: 2px solid #e1e1e1;">
                <div id="rsi_chart" style="height: 100%; width: 100%;"></div>
            </div>
        `;
        
        // Prepare data for TradingView Lightweight Charts
        const dates = data.data.map(d => d.date);
        const pcrVolume = data.data.map(d => d.pcr_volume);
        const pcrOI = data.data.map(d => d.pcr_oi);
        const underlyingPrice = data.data.map(d => d.underlying_price);
        
        // Get RSI data from backend (calculated using pandas-ta)
        const rsiData = data.data
            .map((d, i) => ({
                time: dates[i],
                value: d.rsi
            }))
            .filter(d => d.value !== null && d.value !== undefined);
        
        let thirdLineData, thirdLineLabel;
        
        if (metric === 'vega') {
            if (strike && strike !== 'N/A') {
                thirdLineData = data.data.map(d => d.strike_vega || 0);
                thirdLineLabel = `Strike ${strike} Vega`;
            } else {
                thirdLineData = data.data.map(d => d.avg_vega || 0);
                thirdLineLabel = 'Average Vega (All Strikes)';
            }
        } else {
            thirdLineData = data.data.map(d => d.moneyness);
            thirdLineLabel = 'Moneyness Change';
        }
        
        // Create TradingView Lightweight Chart with TWO separate panels
        const mainChartElement = document.getElementById('tradingview_chart');
        const rsiChartElement = document.getElementById('rsi_chart');
        
        // Main Chart (top panel)
        currentChart = LightweightCharts.createChart(mainChartElement, {
            width: mainChartElement.clientWidth,
            height: mainChartElement.clientHeight,
            layout: {
                background: { color: '#ffffff' },
                textColor: '#333',
                fontSize: 12,
            },
            grid: {
                vertLines: { color: '#f0f0f0' },
                horzLines: { color: '#f0f0f0' },
            },
            crosshair: {
                mode: LightweightCharts.CrosshairMode.Normal,
                vertLine: {
                    width: 1,
                    color: '#758696',
                    style: 3,
                },
                horzLine: {
                    width: 1,
                    color: '#758696',
                    style: 3,
                },
            },
            leftPriceScale: {
                visible: true,
                borderColor: '#d1d4dc',
            },
            rightPriceScale: {
                visible: true,
                borderColor: '#d1d4dc',
            },
            timeScale: {
                visible: false,
                borderColor: '#d1d4dc',
            },
        });
        
        // RSI Chart (bottom panel)
        const rsiChart = LightweightCharts.createChart(rsiChartElement, {
            width: rsiChartElement.clientWidth,
            height: rsiChartElement.clientHeight,
            layout: {
                background: { color: '#ffffff' },
                textColor: '#333',
                fontSize: 12,
            },
            grid: {
                vertLines: { color: '#f0f0f0' },
                horzLines: { color: '#f0f0f0' },
            },
            crosshair: {
                mode: LightweightCharts.CrosshairMode.Normal,
                vertLine: {
                    width: 1,
                    color: '#758696',
                    style: 3,
                },
                horzLine: {
                    width: 1,
                    color: '#758696',
                    style: 3,
                },
            },
            leftPriceScale: {
                visible: true,
                borderColor: '#d1d4dc',
            },
            rightPriceScale: {
                visible: false,
            },
            timeScale: {
                borderColor: '#d1d4dc',
                timeVisible: true,
            },
        });
        
        // Store RSI chart for cleanup
        window.rsiChart = rsiChart;
        
        // Handle window resize dynamically
        const resizeObserver = new ResizeObserver(entries => {
            if (currentChart && window.rsiChart) {
                const mainEl = document.getElementById('tradingview_chart');
                const rsiEl = document.getElementById('rsi_chart');
                if (mainEl && rsiEl) {
                    currentChart.applyOptions({ 
                        width: mainEl.clientWidth,
                        height: mainEl.clientHeight 
                    });
                    window.rsiChart.applyOptions({ 
                        width: rsiEl.clientWidth,
                        height: rsiEl.clientHeight 
                    });
                }
            }
        });
        resizeObserver.observe(chartContainer);
        window.chartResizeObserver = resizeObserver;
        
        // Add PCR Volume line (LEFT axis)
        const pcrVolSeries = currentChart.addLineSeries({
            color: '#2196F3',
            lineWidth: 2,
            title: 'PCR (Volume)',
            priceScaleId: 'left',
        });
        pcrVolSeries.setData(dates.map((date, i) => ({
            time: date,
            value: pcrVolume[i]
        })));
        
        // Add PCR OI line (LEFT axis)
        const pcrOISeries = currentChart.addLineSeries({
            color: '#f44336',
            lineWidth: 2,
            title: 'PCR (OI)',
            priceScaleId: 'left',
        });
        pcrOISeries.setData(dates.map((date, i) => ({
            time: date,
            value: pcrOI[i]
        })));
        
        // Add Underlying Price line (RIGHT axis)
        const underlyingSeries = currentChart.addLineSeries({
            color: '#ff9800',
            lineWidth: 2,
            title: 'Underlying Price',
            priceScaleId: 'right',
        });
        underlyingSeries.setData(dates.map((date, i) => ({
            time: date,
            value: underlyingPrice[i]
        })));
        
        // Add Vega/Moneyness line (LEFT axis with PCR)
        const thirdSeries = currentChart.addLineSeries({
            color: metric === 'vega' ? '#9c27b0' : '#4caf50',
            lineWidth: 2,
            title: thirdLineLabel,
            priceScaleId: 'left',
        });
        thirdSeries.setData(dates.map((date, i) => ({
            time: date,
            value: thirdLineData[i]
        })));
        
        // RSI line in separate panel below (calculated using pandas-ta in backend)
        let rsiSeries = null;
        if (rsiData.length > 0) {
            rsiSeries = rsiChart.addLineSeries({
                color: '#673ab7',
                lineWidth: 2,
                title: 'RSI (14)',
                priceScaleId: 'left',
            });
            rsiSeries.setData(rsiData);
            
            // Configure RSI scale range to show 0-100
            rsiChart.priceScale('left').applyOptions({
                scaleMargins: {
                    top: 0.05,
                    bottom: 0.05,
                },
            });
        }
        
        currentChart.timeScale().fitContent();
        if (rsiChart) {
            rsiChart.timeScale().fitContent();
        }
        
        // Synchronize crosshairs between main chart and RSI chart
        currentChart.subscribeCrosshairMove((param) => {
            if (param.time && rsiChart) {
                rsiChart.setCrosshairPosition(0, param.time, rsiSeries);
            } else if (rsiChart) {
                rsiChart.clearCrosshairPosition();
            }
        });
        
        if (rsiChart) {
            rsiChart.subscribeCrosshairMove((param) => {
                if (param.time) {
                    currentChart.setCrosshairPosition(0, param.time, underlyingSeries);
                } else {
                    currentChart.clearCrosshairPosition();
                }
            });
        }
        
        isLightweightChart = true;
        
        // Create custom legend with toggles in the header
        const legendContainer = document.getElementById('chart-legend-header');
        if (!legendContainer) {
            console.error('Legend container not found');
        } else {
            console.log('Legend container found, creating legend...');
        }
        
        const seriesData = [
            { series: pcrVolSeries, name: 'PCR (Volume)', color: '#2196F3' },
            { series: pcrOISeries, name: 'PCR (OI)', color: '#f44336' },
            { series: underlyingSeries, name: 'Underlying Price', color: '#ff9800' },
            { series: thirdSeries, name: thirdLineLabel, color: metric === 'vega' ? '#9c27b0' : '#4caf50' }
        ];
        
        // Add RSI to legend
        if (rsiSeries) {
            seriesData.push({ series: rsiSeries, name: 'RSI (14)', color: '#673ab7' });
        }
        
        seriesData.forEach((item, index) => {
            const wrapper = document.createElement('div');
            wrapper.style.cssText = 'display: flex; align-items: center; cursor: pointer; white-space: nowrap;';
            
            const checkbox = document.createElement('input');
            checkbox.type = 'checkbox';
            checkbox.checked = true;
            checkbox.id = `legend-${index}`;
            checkbox.style.cssText = 'margin-right: 6px; cursor: pointer;';
            
            const colorBox = document.createElement('span');
            colorBox.style.cssText = `
                display: inline-block;
                width: 16px;
                height: 3px;
                background: ${item.color};
                margin-right: 6px;
                border-radius: 2px;
            `;
            
            const label = document.createElement('label');
            label.htmlFor = `legend-${index}`;
            label.textContent = item.name;
            label.style.cssText = 'cursor: pointer; user-select: none; font-size: 12px;';
            
            checkbox.addEventListener('change', () => {
                if (checkbox.checked) {
                    item.series.applyOptions({ visible: true });
                } else {
                    item.series.applyOptions({ visible: false });
                }
            });
            
            wrapper.appendChild(checkbox);
            wrapper.appendChild(colorBox);
            wrapper.appendChild(label);
            
            if (legendContainer) {
                legendContainer.appendChild(wrapper);
            }
        });
        
    } catch (error) {
        alert('Error loading chart: ' + error);
    }
}


function closeChart() {
    document.getElementById('chartModal').style.display = 'none';
    
    // Clean up resize observer
    if (window.chartResizeObserver) {
        window.chartResizeObserver.disconnect();
        window.chartResizeObserver = null;
    }
    
    if (currentChart) {
        if (isLightweightChart) {
            currentChart.remove();
        }
        currentChart = null;
        isLightweightChart = false;
    }
    
    // Clean up RSI chart
    if (window.rsiChart) {
        window.rsiChart.remove();
        window.rsiChart = null;
    }
}


// Close modal when clicking outside
window.onclick = function(event) {
    const modal = document.getElementById('chartModal');
    if (event.target == modal) {
        closeChart();
    }
}


function exportToExcel() {
    // Get the active tab
    const activeText = document.querySelector('.tab.active').textContent;
    let tabName = 'TOTAL';
    let tableInstance = totalTable;
    
    if (activeText.includes('OTM')) {
        tabName = 'OTM';
        tableInstance = otmTable;
    } else if (activeText.includes('ITM')) {
        tabName = 'ITM';
        tableInstance = itmTable;
    }
    
    if (!tableInstance) {
        alert('No data to export');
        return;
    }
    
    // Get all data from DataTable using API
    const allData = tableInstance.rows().nodes();
    const columns = tableInstance.columns().header().toArray().map(th => th.textContent);
    
    // Create worksheet data
    const wsData = [columns];
    
    // Add all rows
    allData.each(function(row) {
        const rowData = [];
        $(row).find('td').each(function() {
            // Get text content, strip HTML and formatting
            let text = $(this).text().trim();
            rowData.push(text);
        });
        wsData.push(rowData);
    });
    
    // Create workbook and worksheet
    const wb = XLSX.utils.book_new();
    const ws = XLSX.utils.aoa_to_sheet(wsData);
    
    XLSX.utils.book_append_sheet(wb, ws, tabName);
    
    // Download
    const fileName = `Options_${tabName}_${document.getElementById('currentDate').value}.xlsx`;
    XLSX.writeFile(wb, fileName);
}
