/**
 * ============================================================================
 * HOME PAGE - MARKET INDEX CHARTS
 * Real-time chart rendering for NIFTY 50, BANK NIFTY, SENSEX, NIFTY FIN
 *
 * Using Chart.js for lightweight, responsive charts
 * Structure supports easy integration with real-time WebSocket data later
 * ============================================================================
 */

// Chart configuration - optimized for small card displays
const CHART_CONFIG = {
    type: 'line',
    options: {
        responsive: true,
        maintainAspectRatio: false,
        plugins: {
            legend: { display: false },
            tooltip: { enabled: false }
        },
        scales: {
            x: {
                display: false,
                grid: { display: false }
            },
            y: {
                display: false,
                grid: { display: false }
            }
        },
        elements: {
            point: { radius: 0 },
            line: {
                borderWidth: 2,
                tension: 0.4
            }
        },
        interaction: {
            intersect: false,
            mode: 'index'
        },
        animation: {
            duration: 750,
            easing: 'easeInOutQuart'
        }
    }
};

// Global charts storage
const marketCharts = {};

/**
 * Generate dummy time-series data for a market index
 * @param {number} basePrice - Starting price
 * @param {number} volatility - Price fluctuation percentage (0-1)
 * @param {number} trend - Overall trend direction (-1 to 1)
 * @param {number} points - Number of data points
 * @returns {Array} Array of price values
 */
function generateDummyData(basePrice, volatility = 0.02, trend = 0, points = 50) {
    const data = [];
    let currentPrice = basePrice;

    for (let i = 0; i < points; i++) {
        // Random walk with trend
        const randomChange = (Math.random() - 0.5) * volatility * basePrice;
        const trendChange = (trend * basePrice * 0.0001);
        currentPrice += randomChange + trendChange;

        // Keep price positive
        currentPrice = Math.max(currentPrice * 0.95, currentPrice);
        data.push(currentPrice);
    }

    return data;
}

/**
 * Get gradient for chart based on trend direction
 * @param {HTMLCanvasElement} canvas - Chart canvas element
 * @param {boolean} isPositive - Is the trend positive?
 * @returns {CanvasGradient} Gradient for chart
 */
function getChartGradient(canvas, isPositive) {
    const ctx = canvas.getContext('2d');
    const gradient = ctx.createLinearGradient(0, 0, 0, canvas.height);

    if (isPositive) {
        gradient.addColorStop(0, 'rgba(0, 200, 83, 0.3)');
        gradient.addColorStop(1, 'rgba(0, 200, 83, 0.01)');
        return {
            borderColor: '#00c853',
            backgroundColor: gradient
        };
    } else {
        gradient.addColorStop(0, 'rgba(255, 107, 107, 0.3)');
        gradient.addColorStop(1, 'rgba(255, 107, 107, 0.01)');
        return {
            borderColor: '#ff6b6b',
            backgroundColor: gradient
        };
    }
}

/**
 * Initialize a market index chart
 * @param {string} chartId - Canvas element ID
 * @param {Object} indexData - Market index data object
 * @param {boolean} isLargeChart - Whether this is the large combined chart
 */
function initMarketChart(chartId, indexData, isLargeChart = false) {
    const canvas = document.getElementById(chartId);
    if (!canvas) {
        console.error(`Chart canvas not found: ${chartId}`);
        return;
    }

    const ctx = canvas.getContext('2d');
    const isPositive = indexData.change >= 0;
    const colors = getChartGradient(canvas, isPositive);

    // Generate dummy historical data
    const numPoints = isLargeChart ? 100 : 50;
    const chartData = generateDummyData(
        indexData.value,
        0.015, // volatility
        isPositive ? 0.3 : -0.3, // trend
        numPoints
    );

    // Generate time labels for large chart
    const timeLabels = [];
    if (isLargeChart) {
        const startHour = 9;
        const startMinute = 0;
        const endHour = 15;
        const endMinute = 30;

        for (let i = 0; i < numPoints; i++) {
            const totalMinutes = startMinute + (i * ((endHour * 60 + endMinute) - (startHour * 60 + startMinute)) / (numPoints - 1));
            const hour = Math.floor((startHour * 60 + totalMinutes) / 60);
            const minute = Math.floor((startHour * 60 + totalMinutes) % 60);
            timeLabels.push(`${hour.toString().padStart(2, '0')}:${minute.toString().padStart(2, '0')}`);
        }
    }

    // Chart options - different for large vs small charts
    const chartOptions = isLargeChart ? {
        responsive: true,
        maintainAspectRatio: false,
        interaction: {
            intersect: false,
            mode: 'index'
        },
        plugins: {
            legend: { display: false },
            tooltip: {
                enabled: true,
                backgroundColor: 'rgba(0, 0, 0, 0.8)',
                padding: 10,
                bodyFont: { size: 12 },
                displayColors: false
            }
        },
        scales: {
            x: {
                display: true,
                grid: {
                    display: false,
                    drawBorder: false
                },
                ticks: {
                    maxTicksLimit: 8,
                    font: { size: 11 },
                    color: '#9ca3af'
                }
            },
            y: {
                display: true,
                position: 'right',
                grid: {
                    color: '#f3f4f6',
                    drawBorder: false
                },
                ticks: {
                    maxTicksLimit: 6,
                    font: { size: 11 },
                    color: '#9ca3af',
                    callback: function(value) {
                        return value.toLocaleString('en-IN', {maximumFractionDigits: 0});
                    }
                }
            }
        },
        elements: {
            line: {
                borderWidth: 2,
                tension: 0.4
            },
            point: {
                radius: 0,
                hitRadius: 10,
                hoverRadius: 4
            }
        },
        animation: {
            duration: 750,
            easing: 'easeInOutQuart'
        }
    } : CHART_CONFIG.options;

    // Create chart
    marketCharts[chartId] = new Chart(ctx, {
        type: 'line',
        data: {
            labels: isLargeChart ? timeLabels : Array(chartData.length).fill(''),
            datasets: [{
                data: chartData,
                borderColor: colors.borderColor,
                backgroundColor: colors.backgroundColor,
                fill: true
            }]
        },
        options: chartOptions
    });
}

/**
 * Update chart with new data point
 * @param {string} chartId - Canvas element ID
 * @param {number} newValue - New price value
 */
function updateChartData(chartId, newValue) {
    const chart = marketCharts[chartId];
    if (!chart) return;

    // Add new data point
    chart.data.datasets[0].data.push(newValue);

    // Remove oldest point if more than max points
    const maxPoints = chartId === 'marketIndexChart' ? 100 : 50;
    if (chart.data.datasets[0].data.length > maxPoints) {
        chart.data.datasets[0].data.shift();
    }

    chart.update('none'); // Update without animation for smooth real-time feel
}

/**
 * Initialize all market charts on page load
 */
function initAllMarketCharts() {
    // Market data passed from Flask template
    const defaultMarketData = {
        nifty50: { value: 24677.80, change: 0.42 },
        banknifty: { value: 51234.50, change: -0.18 },
        sensex: { value: 83758.43, change: 1.23 },
        niftyfin: { value: 23890.40, change: 0.67 }
    };

    const data = typeof marketData !== 'undefined' ? marketData : defaultMarketData;

    // Initialize the large chart with NIFTY 50 data
    initMarketChart('marketIndexChart', data.nifty50, true);

    // Real-time updates handled by home_live_updates.js
}

// Initialize charts when DOM is ready
document.addEventListener('DOMContentLoaded', () => {
    initAllMarketCharts();
});
