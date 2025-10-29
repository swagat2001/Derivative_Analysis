// Stock Detail Page JavaScript - Redesigned Version

const ticker = document.getElementById('symbolName').textContent;
let priceChart = null;
let expiryDatesLoaded = false;
let availableTradingDates = [];

// Initialize
document.addEventListener('DOMContentLoaded', function() {
    populateSymbolDropdown();
    loadAvailableDates();
});

async function loadAvailableDates() {
    try {
        const response = await fetch('/get_available_trading_dates');
        const data = await response.json();
        
        if (data.dates && data.dates.length > 0) {
            availableTradingDates = data.dates;
            setDefaultDate(data.dates[0]);
        } else {
            setDefaultDate();
        }
        
        // Load expiry dates after setting default date
        await loadExpiryDates();
    } catch (error) {
        console.error('Error loading trading dates:', error);
        setDefaultDate();
        await loadExpiryDates();
    }
}

function setDefaultDate(defaultDate = null) {
    const dateInput = document.getElementById('historicalDate');
    const today = new Date().toISOString().split('T')[0];
    
    if (defaultDate) {
        dateInput.value = defaultDate;
        dateInput.max = today;
        
        if (availableTradingDates.length > 0) {
            const oldestDate = availableTradingDates[availableTradingDates.length - 1];
            dateInput.min = oldestDate;
        }
    } else {
        dateInput.value = today;
        dateInput.max = today;
    }
}

async function loadExpiryDates() {
    try {
        const selectedDate = document.getElementById('historicalDate').value;
        
        let url = `/get_expiry_dates?ticker=${ticker}`;
        if (selectedDate) {
            url += `&date=${selectedDate}`;
        }
        
        const response = await fetch(url);
        const data = await response.json();
        
        const select = document.getElementById('expirySelect');
        select.innerHTML = '';
        
        if (!data.expiry_dates || data.expiry_dates.length === 0) {
            const opt = document.createElement('option');
            opt.value = '';
            opt.textContent = 'No expiries available';
            select.appendChild(opt);
            console.log(`⚠️ No expiry dates for ${ticker} on ${selectedDate}`);
            expiryDatesLoaded = true;
        } else {
            data.expiry_dates.forEach((exp, idx) => {
                const opt = document.createElement('option');
                opt.value = exp;
                // Format as DDMMMYY
                const date = new Date(exp);
                const day = String(date.getDate()).padStart(2, '0');
                const monthNames = ['JAN', 'FEB', 'MAR', 'APR', 'MAY', 'JUN', 'JUL', 'AUG', 'SEP', 'OCT', 'NOV', 'DEC'];
                const month = monthNames[date.getMonth()];
                const year = String(date.getFullYear()).slice(-2);
                opt.textContent = `${day}${month}${year}`;
                if (idx === 0) opt.selected = true;
                select.appendChild(opt);
            });
            console.log(`✅ Loaded ${data.expiry_dates.length} expiry dates`);
            expiryDatesLoaded = true;
            
            // AUTO-LOAD DATA after expiry dates are loaded
            await loadStockData();
        }
        
    } catch (error) {
        console.error('Error loading expiry dates:', error);
        const select = document.getElementById('expirySelect');
        select.innerHTML = '<option value="">Error loading</option>';
        expiryDatesLoaded = true;
    }
}

function populateSymbolDropdown() {
    fetch('/get_available_tickers')
        .then(r => r.json())
        .then(tickers => {
            const select = document.getElementById('symbolSelect');
            select.innerHTML = '';
            tickers.forEach(t => {
                const opt = document.createElement('option');
                opt.value = t;
                opt.textContent = t;
                opt.selected = (t === ticker);
                select.appendChild(opt);
            });
        })
        .catch(err => console.error('Error loading tickers:', err));
}

function changeSymbol() {
    const newSymbol = document.getElementById('symbolSelect').value;
    if (newSymbol) {
        window.location.href = `/stock/${newSymbol}`;
    }
}

async function onDateChange() {
    // Reload expiry dates when date changes
    await loadExpiryDates();
    // Then load the stock data
    await loadStockData();
}

async function loadStockData() {
    if (!expiryDatesLoaded) {
        return;
    }
    
    const expiry = document.getElementById('expirySelect').value;
    const historicalDate = document.getElementById('historicalDate').value;
    
    if (!historicalDate) {
        alert('Please select a date');
        return;
    }
    
    if (!expiry) {
        console.log('No expiry selected');
        return;
    }
    
    // Validate date
    const selectedDate = new Date(historicalDate);
    const today = new Date();
    today.setHours(0, 0, 0, 0);
    
    if (selectedDate > today) {
        alert('Cannot select future dates');
        if (availableTradingDates.length > 0) {
            document.getElementById('historicalDate').value = availableTradingDates[0];
        }
        return;
    }
    
    // Show loading states
    document.getElementById('priceChartContainer').innerHTML = '<div class="loading">Loading chart...</div>';
    document.getElementById('optionChainTable').innerHTML = '<div class="loading">Loading option chain...</div>';
    
    try {
        let url = `/get_stock_data?ticker=${ticker}&mode=historical&date=${historicalDate}`;
        if (expiry && expiry !== 'all') {
            url += `&expiry=${expiry}`;
        }
        
        console.log('Fetching:', url);
        
        const response = await fetch(url);
        
        if (!response.ok) {
            const errorData = await response.json();
            throw new Error(errorData.error || `HTTP ${response.status}`);
        }
        
        const data = await response.json();
        
        console.log('Received data:', data);
        
        if (data.error) {
            alert(`Error: ${data.error}`);
            return;
        }
        
        // Update all sections
        updateExpiryTable(data.expiry_dates || [], historicalDate);
        updateGaugesAndStats(data.stats, data.option_chain, data.last_updated);
        updatePriceChart(data.price_data);
        updateOptionChain(data.option_chain, data.stats);
        
    } catch (error) {
        console.error('Error loading data:', error);
        alert(`Error loading data: ${error.message}`);
    }
}

function updateExpiryTable(expiryDates, currentDate) {
    const tbody = document.getElementById('expiryTableBody');
    
    if (!expiryDates || expiryDates.length === 0) {
        tbody.innerHTML = '<tr><td colspan="6" class="loading">No expiry data available</td></tr>';
        return;
    }
    
    // Fetch detailed expiry data
    fetch(`/get_expiry_data_detailed?ticker=${ticker}&date=${currentDate}`)
        .then(response => response.json())
        .then(data => {
            if (data.error) {
                tbody.innerHTML = '<tr><td colspan="6" class="loading">Error loading data</td></tr>';
                return;
            }
            
            // Update Fair Price and Lot Size
            let fairPriceText = data.fair_price || '-';
            if (data.expiry_data && data.expiry_data.length > 0 && data.expiry_data[0].expiry) {
                const date = new Date(data.expiry_data[0].expiry);
                const day = String(date.getDate()).padStart(2, '0');
                const monthNames = ['JAN', 'FEB', 'MAR', 'APR', 'MAY', 'JUN', 'JUL', 'AUG', 'SEP', 'OCT', 'NOV', 'DEC'];
                const month = monthNames[date.getMonth()];
                const year = String(date.getFullYear()).slice(-2);
                const formattedExpiry = `${day}${month}${year}`;
                fairPriceText = `(${formattedExpiry}) : ${data.fair_price.toFixed(2)}`;
            }
            document.getElementById('fairPrice').textContent = fairPriceText;
            document.getElementById('lotSize').textContent = data.lot_size || '-';
            
            // Build table rows
            let html = '';
            data.expiry_data.forEach((expiry) => {
                // Format expiry date
                let formattedExpiry = '-';
                if (expiry.expiry) {
                    const date = new Date(expiry.expiry);
                    const day = String(date.getDate()).padStart(2, '0');
                    const monthNames = ['JAN', 'FEB', 'MAR', 'APR', 'MAY', 'JUN', 'JUL', 'AUG', 'SEP', 'OCT', 'NOV', 'DEC'];
                    const month = monthNames[date.getMonth()];
                    const year = String(date.getFullYear()).slice(-2);
                    formattedExpiry = `${day}${month}${year}`;
                }
                
                const priceChgPercent = expiry.price_chg_percent || 0;
                const priceChgClass = priceChgPercent >= 0 ? 'positive' : 'negative';
                const priceChgSymbol = priceChgPercent >= 0 ? '▲' : '▼';
                
                const oiChgPercent = expiry.oi_chg_percent || 0;
                const oiChgClass = oiChgPercent >= 0 ? 'positive' : 'negative';
                const oiChgSymbol = oiChgPercent >= 0 ? '▲' : '▼';
                
                html += `<tr>`;
                html += `<td><strong>${formattedExpiry}</strong></td>`;
                html += `<td>${expiry.price.toFixed(2)}</td>`;
                html += `<td class="${priceChgClass}">${priceChgSymbol} ${Math.abs(priceChgPercent).toFixed(2)}%</td>`;
                html += `<td>${formatNumber(expiry.volume)}</td>`;
                html += `<td>${formatNumber(expiry.oi)}</td>`;
                html += `<td class="${oiChgClass}">${oiChgSymbol} ${Math.abs(oiChgPercent).toFixed(2)}%</td>`;
                html += `</tr>`;
            });
            
            tbody.innerHTML = html;
        })
        .catch(error => {
            console.error('Error:', error);
            tbody.innerHTML = '<tr><td colspan="6" class="loading">Error loading data</td></tr>';
        });
}

function updateGaugesAndStats(stats, optionChain, lastUpdated) {
    // Update Last Updated
    if (lastUpdated) {
        const date = new Date(lastUpdated);
        const formatted = date.toLocaleString('en-GB', {
            day: '2-digit',
            month: 'short',
            year: 'numeric',
            hour: '2-digit',
            minute: '2-digit'
        });
        document.getElementById('lastUpdated').textContent = formatted;
    }
    
    // DYNAMIC GAUGES - ONLY IV AND PCR
    
    // 1. IV Gauge - Use 'iv' column from database
    if (optionChain && optionChain.length > 0) {
        // Get IV values from option chain
        const callIVs = optionChain.map(row => row.call_iv || 0).filter(v => v > 0);
        const putIVs = optionChain.map(row => row.put_iv || 0).filter(v => v > 0);
        const allIVs = [...callIVs, ...putIVs];
        
        if (allIVs.length > 0) {
            let avgIV = allIVs.reduce((a, b) => a + b, 0) / allIVs.length;
            const minIV = Math.min(...allIVs);
            const maxIV = Math.max(...allIVs);
            
            // Fix #3: Add scaling guard - if avgIV > 0 && avgIV < 1, multiply by 100
            if (avgIV > 0 && avgIV < 1) {
                avgIV = avgIV * 100;
            }
            
            document.getElementById('ivMin').textContent = minIV.toFixed(2);
            document.getElementById('ivMax').textContent = maxIV.toFixed(2);
            
            // Calculate IVR (IV Rank)
            const ivr = ((avgIV - minIV) / (maxIV - minIV)) * 100;
            const ivp = avgIV * 3.5; // IVP approximation
            
            document.getElementById('ivRange').textContent = `IVR: ${ivr.toFixed(2)}  IVP: ${ivp.toFixed(2)}`;
            updateCircularGauge('ivCircle', 'ivGauge', avgIV, minIV, maxIV);
        } else {
            document.getElementById('ivGauge').textContent = '-';
            document.getElementById('ivRange').textContent = '-';
        }
    } else {
        document.getElementById('ivGauge').textContent = '-';
        document.getElementById('ivRange').textContent = '-';
    }
    
    // 2. PCR Gauge - From stats (already dynamic from DB)
    if (stats && stats.pcr_oi) {
        const pcrMin = 0.5;
        const pcrMax = 2.0;
        document.getElementById('pcrMin').textContent = pcrMin.toFixed(2);
        document.getElementById('pcrMax').textContent = pcrMax.toFixed(2);
        updateCircularGauge('pcrCircle', 'pcrGauge', stats.pcr_oi, pcrMin, pcrMax);
    } else {
        document.getElementById('pcrGauge').textContent = '-';
    }
    
    // Update Stats - ALL FROM DATABASE
    document.getElementById('totalCeOi').textContent = stats ? formatNumber(stats.total_ce_oi) : '-';
    document.getElementById('totalPeOi').textContent = stats ? formatNumber(stats.total_pe_oi) : '-';
    document.getElementById('totalCeOiChg').textContent = stats ? formatNumber(stats.total_ce_oi_chg) : '-';
    document.getElementById('totalPeOiChg').textContent = stats ? formatNumber(stats.total_pe_oi_chg) : '-';
    
    if (stats) {
        const diffOi = stats.total_pe_oi - stats.total_ce_oi;
        const diffOiChg = stats.total_pe_oi_chg - stats.total_ce_oi_chg;
        document.getElementById('diffPeCeOi').textContent = formatNumber(diffOi);
        document.getElementById('diffPeCeOiChg').textContent = formatNumber(diffOiChg);
        
        // Fix #5: Use conventional PCR mapping
        // PCR > 1 → More Puts → Bearish
        // PCR < 1 → More Calls → Bullish
        let trend = 'Neutral';
        let trendChg = 'Neutral';
        if (stats.pcr_oi > 1) trend = 'Bullish';
        else if (stats.pcr_oi < 1) trend = 'Bearish';

        // Trend OI Chg based on change in PE-CE difference
        if (diffOiChg > 0) trendChg = 'Bullish';  // More PE being added
        else if (diffOiChg < 0) trendChg = 'Bearish';  // More CE being added

        // Apply trend text with color
        setTrendColor(document.getElementById('trendOi'), trend);
        setTrendColor(document.getElementById('trendOiChg'), trendChg);
    } else {
        document.getElementById('diffPeCeOi').textContent = '-';
        document.getElementById('diffPeCeOiChg').textContent = '-';
        document.getElementById('trendOi').textContent = '-';
        document.getElementById('trendOiChg').textContent = '-';
    }
    
    // Calculate Max Strikes from option chain (using OpnIntrst column)
    if (optionChain && optionChain.length > 0) {
        let maxCeOi = 0, maxCeOiStrike = 0;
        let maxPeOi = 0, maxPeOiStrike = 0;
        let maxCeOiChg = -Infinity, maxCeOiChgStrike = 0;
        let maxPeOiChg = -Infinity, maxPeOiChgStrike = 0;
        
        optionChain.forEach(row => {
            // Using OpnIntrst column from DB
            if (row.call_oi > maxCeOi) {
                maxCeOi = row.call_oi;
                maxCeOiStrike = row.strike; // StrkPric column
            }
            if (row.put_oi > maxPeOi) {
                maxPeOi = row.put_oi;
                maxPeOiStrike = row.strike;
            }
            // Using ChngInOpnIntrst column from DB
            if (row.call_oi_chg > maxCeOiChg) {
                maxCeOiChg = row.call_oi_chg;
                maxCeOiChgStrike = row.strike;
            }
            if (row.put_oi_chg > maxPeOiChg) {
                maxPeOiChg = row.put_oi_chg;
                maxPeOiChgStrike = row.strike;
            }
        });
        
        document.getElementById('maxCeOiStrike').textContent = maxCeOiStrike;
        document.getElementById('maxPeOiStrike').textContent = maxPeOiStrike;
        document.getElementById('maxCeOiChgStrike').textContent = maxCeOiChgStrike;
        document.getElementById('maxPeOiChgStrike').textContent = maxPeOiChgStrike;
    } else {
        document.getElementById('maxCeOiStrike').textContent = '-';
        document.getElementById('maxPeOiStrike').textContent = '-';
        document.getElementById('maxCeOiChgStrike').textContent = '-';
        document.getElementById('maxPeOiChgStrike').textContent = '-';
    }
}

// Max Pain calculation using StrkPric and OpnIntrst from database
function updateCircularGauge(circleId, valueId, value, min, max) {
    const circle = document.getElementById(circleId);
    if (!circle) return;
    
    // Handle edge cases
    if (max === min) {
        circle.style.strokeDashoffset = 314; // Empty gauge
        if (valueId) document.getElementById(valueId).textContent = '-';
        return;
    }
    
    // Calculate percentage (clamp between 0-100)
    const percentage = Math.max(0, Math.min(100, ((value - min) / (max - min)) * 100));
    const circumference = 314; // 2 * PI * radius (2 * 3.14159 * 50)
    const offset = circumference - (circumference * percentage) / 100;
    
    // Animate the gauge
    circle.style.strokeDashoffset = offset;
    circle.style.transition = 'stroke-dashoffset 1s ease-out';
    
    // Update the value text
    if (valueId) {
        document.getElementById(valueId).textContent = value.toFixed(2);
    }
}

function updatePriceChart(priceData) {
    const container = document.getElementById('priceChartContainer');
    container.innerHTML = '';
    
    if (!priceData || priceData.length === 0) {
        container.innerHTML = '<div class="loading">No price data available</div>';
        return;
    }
    
    if (priceChart) {
        priceChart.remove();
        priceChart = null;
    }
    
    priceChart = LightweightCharts.createChart(container, {
        width: container.clientWidth,
        height: 450,
        layout: {
            background: { color: '#ffffff' },
            textColor: '#333',
        },
        grid: {
            vertLines: { color: '#f0f0f0' },
            horzLines: { color: '#f0f0f0' },
        },
        crosshair: {
            mode: LightweightCharts.CrosshairMode.Normal,
        },
        timeScale: {
            borderColor: '#d1d4dc',
            timeVisible: true,
            secondsVisible: false,
        },
        rightPriceScale: {
            borderColor: '#d1d4dc',
        },
    });
    
    // Price Line
    const priceSeries = priceChart.addLineSeries({
        color: '#2962FF',
        lineWidth: 2,
        title: 'Price',
    });
    priceSeries.setData(priceData.map(d => ({
        time: d.time,
        value: d.close
    })));
    
    // VWAP Line
    const vwapSeries = priceChart.addLineSeries({
        color: '#FF6D00',
        lineWidth: 2,
        title: 'VWAP',
    });
    vwapSeries.setData(priceData.map(d => ({
        time: d.time,
        value: d.vwap || d.close
    })));
    
    // Volume
    const volumeSeries = priceChart.addHistogramSeries({
        color: '#26a69a',
        priceFormat: { type: 'volume' },
        priceScaleId: '',
        scaleMargins: {
            top: 0.8,
            bottom: 0,
        },
    });
    volumeSeries.setData(priceData.map(d => ({
        time: d.time,
        value: d.volume || 0,
        color: d.close >= d.open ? 'rgba(38, 166, 154, 0.5)' : 'rgba(239, 83, 80, 0.5)'
    })));
    
    priceChart.timeScale().fitContent();
}

function updateOptionChain(optionChain, stats) {
    const container = document.getElementById('optionChainTable');
    
    if (!optionChain || optionChain.length === 0) {
        container.innerHTML = '<div class="loading">No option chain data</div>';
        return;
    }
    
    // Get current underlying price - calculate from mid strikes
    let underlyingPrice = 0;
    if (optionChain.length > 0) {
        const midIndex = Math.floor(optionChain.length / 2);
        underlyingPrice = optionChain[midIndex].strike;
    }
    
    let html = '<table class="option-chain-table"><thead><tr>';
    html += '<th colspan="4" class="call-header">CALL</th>';
    html += '<th rowspan="2" class="strike-header">Strike Price</th>';
    html += '<th colspan="4" class="put-header">PUT</th>';
    html += '</tr><tr>';
    html += '<th class="call-header">IV</th>';
    html += '<th class="call-header">OI Chg</th>';
    html += '<th class="call-header">OI</th>';
    html += '<th class="call-header">Price</th>';
    html += '<th class="put-header">Price</th>';
    html += '<th class="put-header">OI</th>';
    html += '<th class="put-header">OI Chg</th>';
    html += '<th class="put-header">IV</th>';
    html += '</tr></thead><tbody>';
    
    // Find ATM strike (nearest to underlying)
    let atmStrike = 0;
    let minDiff = Infinity;
    optionChain.forEach(row => {
        const diff = Math.abs(row.strike - underlyingPrice);
        if (diff < minDiff) {
            minDiff = diff;
            atmStrike = row.strike;
        }
    });
    
    optionChain.forEach(row => {
        html += '<tr>';
        // Use actual IV from backend (call_iv and put_iv)
        html += `<td>${row.call_iv > 0 ? row.call_iv.toFixed(2) : '-'}</td>`;
        html += `<td class="${row.call_oi_chg >= 0 ? 'oi-positive' : 'oi-negative'}">${formatNumberShort(row.call_oi_chg)}</td>`;
        html += `<td>${formatNumberShort(row.call_oi)}</td>`;
        html += `<td>${row.call_price.toFixed(2)}</td>`;
        
        // Strike cell with ITM/ATM highlighting
        let strikeClass = 'strike-cell';
        if (row.strike === atmStrike) {
            strikeClass = 'strike-atm';
        } else if (Math.abs(row.strike - underlyingPrice) < (underlyingPrice * 0.02)) {
            strikeClass = 'strike-itm';
        }
        html += `<td class="${strikeClass}">${row.strike}</td>`;
        
        html += `<td>${row.put_price.toFixed(2)}</td>`;
        html += `<td>${formatNumberShort(row.put_oi)}</td>`;
        html += `<td class="${row.put_oi_chg >= 0 ? 'oi-positive' : 'oi-negative'}">${formatNumberShort(row.put_oi_chg)}</td>`;
        html += `<td>${row.put_iv > 0 ? row.put_iv.toFixed(2) : '-'}</td>`;
        html += '</tr>';
    });
    
    html += '</tbody></table>';
    container.innerHTML = html;
}


function setTrendColor(element, trend) {
    element.textContent = trend;
    if (trend.toLowerCase() === "bullish") {
        element.style.color = "green";
        element.style.fontWeight = "bold";
    } else if (trend.toLowerCase() === "bearish") {
        element.style.color = "red";
        element.style.fontWeight = "bold";
    } else {
        element.style.color = "goldenrod";
        element.style.fontWeight = "bold";
    }
}

function formatNumber(val) {
    if (val === 0) return '0';
    const num = parseFloat(val);
    if (isNaN(num)) return val;
    if (Math.abs(num) >= 1e7) return (num/1e7).toFixed(2) + ' Cr';
    if (Math.abs(num) >= 1e5) return (num/1e5).toFixed(2) + ' L';
    if (Math.abs(num) >= 1e3) return (num/1e3).toFixed(2) + ' K';
    return num.toFixed(2);
}

function formatNumberShort(val) {
    if (val === 0) return '0';
    const num = parseFloat(val);
    if (isNaN(num)) return val;
    if (Math.abs(num) >= 1e5) return (num/1e5).toFixed(2) + ' L';
    if (Math.abs(num) >= 1e3) return (num/1e3).toFixed(2) + ' K';
    return num.toFixed(0);
}