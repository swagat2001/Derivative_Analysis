let currentChart = null;
let currentRSIChart = null;

function openChartModal(el) {
    const modal = document.getElementById('chartModal');
    const title = document.getElementById('modalChartTitle');
    const loading = document.getElementById('chartLoading');
    const canvas = document.getElementById('chartCanvas');
    const legend = document.getElementById('chartLegend');
    
    modal.style.display = 'block';
    loading.style.display = 'flex';
    canvas.style.display = 'none';
    legend.innerHTML = '';
    
    const stock = el.getAttribute('data-stock');
    const optionType = el.getAttribute('data-option-type');
    const metric = el.getAttribute('data-metric');
    const strike = el.getAttribute('data-strike') || null;
    const date = el.getAttribute('data-date');
    
    const metricLabel = metric === 'money' ? 'Money' : 'Vega';
    const optLabel = optionType === 'call' ? 'Call' : 'Put';
    title.textContent = `${stock} - ${optLabel} ${metricLabel}${strike ? ' @ ' + strike : ''}`;
    
    const params = new URLSearchParams({ticker: stock, option_type: optionType, metric: metric, date: date});
    if (strike && strike !== 'N/A') params.append('strike', strike);
    
    fetch(`/api/historical-chart-data?${params}`)
        .then(r => r.json())
        .then(result => {
            loading.style.display = 'none';
            if (result.success && result.data.length > 0) {
                renderChart(result.data);
            } else {
                canvas.innerHTML = '<div class="chart-loading">No data available</div>';
                canvas.style.display = 'block';
            }
        })
        .catch(() => {
            loading.style.display = 'none';
            canvas.innerHTML = '<div class="chart-loading">Error loading chart</div>';
            canvas.style.display = 'block';
        });
}

function renderChart(data) {
    const canvas = document.getElementById('chartCanvas');
    const legend = document.getElementById('chartLegend');
    canvas.style.display = 'block';
    canvas.innerHTML = '';
    legend.innerHTML = '';
    
    const mainDiv = document.createElement('div');
    mainDiv.style.height = '320px';
    mainDiv.style.marginBottom = '10px';
    canvas.appendChild(mainDiv);
    
    currentChart = LightweightCharts.createChart(mainDiv, {
        layout: {background: {color: '#fff'}, textColor: '#333'},
        grid: {vertLines: {color: '#e5e7eb'}, horzLines: {color: '#e5e7eb'}},
        width: mainDiv.clientWidth,
        height: 320,
        timeScale: {timeVisible: true, secondsVisible: false},
        rightPriceScale: {
            visible: true,
            scaleMargins: {top: 0.1, bottom: 0.1}
        },
        leftPriceScale: {
            visible: true,
            scaleMargins: {top: 0.1, bottom: 0.1}
        }
    });
    
    // PCR Volume (left scale)
    const s1 = currentChart.addLineSeries({
        color: '#2196F3', 
        lineWidth: 2,
        priceScaleId: 'left'
    });
    
    // PCR OI (left scale)
    const s2 = currentChart.addLineSeries({
        color: '#f44336', 
        lineWidth: 2,
        priceScaleId: 'left'
    });
    
    // Underlying Price (right scale)
    const s3 = currentChart.addLineSeries({
        color: '#ff9800', 
        lineWidth: 2, 
        priceScaleId: 'right'
    });
    
    // Metric (left scale for vega, right for money)
    const metricPriceScale = data[0].metric_label.includes('Money') ? 'right' : 'left';
    const s4 = currentChart.addLineSeries({
        color: '#9c27b0', 
        lineWidth: 2,
        priceScaleId: metricPriceScale
    });
    
    const chartData = data.map(d => ({
        time: d.date, 
        pcr_vol: d.pcr_volume, 
        pcr_oi: d.pcr_oi, 
        underlying: d.underlying_price, 
        metric: d.value, 
        rsi: d.rsi
    }));
    
    s1.setData(chartData.map(d => ({time: d.time, value: d.pcr_vol})));
    s2.setData(chartData.map(d => ({time: d.time, value: d.pcr_oi})));
    s3.setData(chartData.map(d => ({time: d.time, value: d.underlying})));
    s4.setData(chartData.map(d => ({time: d.time, value: d.metric})));
    
    currentChart.timeScale().fitContent();
    
    // Create legend
    const seriesConfig = [
        {series: s1, name: 'PCR (Volume)', color: '#2196F3'},
        {series: s2, name: 'PCR (OI)', color: '#f44336'},
        {series: s3, name: 'Underlying Price', color: '#ff9800'},
        {series: s4, name: data[0].metric_label, color: '#9c27b0'}
    ];
    
    seriesConfig.forEach((item, idx) => {
        const wrapper = document.createElement('div');
        wrapper.className = 'legend-item';
        
        const checkbox = document.createElement('input');
        checkbox.type = 'checkbox';
        checkbox.checked = true;
        checkbox.id = `legend-${idx}`;
        
        const colorBox = document.createElement('span');
        colorBox.className = 'legend-color';
        colorBox.style.backgroundColor = item.color;
        
        const label = document.createElement('label');
        label.htmlFor = `legend-${idx}`;
        label.textContent = item.name;
        
        checkbox.addEventListener('change', () => {
            item.series.applyOptions({visible: checkbox.checked});
        });
        
        wrapper.appendChild(checkbox);
        wrapper.appendChild(colorBox);
        wrapper.appendChild(label);
        legend.appendChild(wrapper);
    });
    
    // RSI Chart
    const hasRSI = chartData.some(d => d.rsi !== null && d.rsi !== undefined);
    if (hasRSI) {
        const rsiDiv = document.createElement('div');
        rsiDiv.style.height = '120px';
        canvas.appendChild(rsiDiv);
        
        currentRSIChart = LightweightCharts.createChart(rsiDiv, {
            layout: {background: {color: '#fff'}, textColor: '#333'},
            grid: {vertLines: {color: '#e5e7eb'}, horzLines: {color: '#e5e7eb'}},
            width: rsiDiv.clientWidth,
            height: 120,
            timeScale: {visible: false}
        });
        
        const rsiSeries = currentRSIChart.addLineSeries({color: '#673ab7', lineWidth: 2});
        const rsiData = chartData.filter(d => d.rsi !== null && d.rsi !== undefined).map(d => ({time: d.time, value: d.rsi}));
        rsiSeries.setData(rsiData);
        currentRSIChart.timeScale().fitContent();
        
        // Add RSI to legend
        const rsiWrapper = document.createElement('div');
        rsiWrapper.className = 'legend-item';
        
        const rsiCheckbox = document.createElement('input');
        rsiCheckbox.type = 'checkbox';
        rsiCheckbox.checked = true;
        rsiCheckbox.id = 'legend-rsi';
        
        const rsiColorBox = document.createElement('span');
        rsiColorBox.className = 'legend-color';
        rsiColorBox.style.backgroundColor = '#673ab7';
        
        const rsiLabel = document.createElement('label');
        rsiLabel.htmlFor = 'legend-rsi';
        rsiLabel.textContent = 'RSI (14)';
        
        rsiCheckbox.addEventListener('change', () => {
            rsiSeries.applyOptions({visible: rsiCheckbox.checked});
        });
        
        rsiWrapper.appendChild(rsiCheckbox);
        rsiWrapper.appendChild(rsiColorBox);
        rsiWrapper.appendChild(rsiLabel);
        legend.appendChild(rsiWrapper);
        
        // Sync crosshairs
        currentChart.subscribeCrosshairMove(param => {
            if (param.time) {
                currentRSIChart.setCrosshairPosition(0, param.time, rsiSeries);
            } else {
                currentRSIChart.clearCrosshairPosition();
            }
        });
        
        currentRSIChart.subscribeCrosshairMove(param => {
            if (param.time) {
                currentChart.setCrosshairPosition(0, param.time, s3);
            } else {
                currentChart.clearCrosshairPosition();
            }
        });
    }
    
    // Handle resize
    window.addEventListener('resize', () => {
        if (currentChart) currentChart.applyOptions({width: mainDiv.clientWidth});
        if (currentRSIChart) currentRSIChart.applyOptions({width: mainDiv.clientWidth});
    });
}

function closeChartModal() {
    document.getElementById('chartModal').style.display = 'none';
    if (currentChart) { 
        currentChart.remove(); 
        currentChart = null; 
    }
    if (currentRSIChart) { 
        currentRSIChart.remove(); 
        currentRSIChart = null; 
    }
}

window.onclick = e => { 
    if (e.target.id === 'chartModal') closeChartModal(); 
};

document.addEventListener('keydown', e => { 
    if (e.key === 'Escape') closeChartModal(); 
});