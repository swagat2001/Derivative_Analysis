/**
 * Live Indices Data Updates
 * Fetches real-time data from broker WebSocket (via Flask API)
 */

class LiveIndicesUpdater {
    constructor() {
        this.apiUrl = '/api/live-indices';
        this.updateInterval = 1000; // 1 second
        this.intervalId = null;
        this.isRunning = false;
        this.selectedIndex = 'nifty50'; // Default selected index
        this.latestData = null;
        this.initialized = false;
    }

    async fetchData() {
        try {
            const response = await fetch(this.apiUrl);
            if (!response.ok) {
                return null;
            }
            const data = await response.json();
            return data;
        } catch (error) {
            return null;
        }
    }

    updateIndexCard(indexKey, data) {
        const cardMap = {
            'nifty50': { valueId: 'nifty50Value', changeId: 'nifty50Change' },
            'banknifty': { valueId: 'bankniftyValue', changeId: 'bankniftyChange' },
            'sensex': { valueId: 'sensexValue', changeId: 'sensexChange' },
            'niftyfin': { valueId: 'niftyfinValue', changeId: 'niftyfinChange' },
            'niftynext50': { valueId: 'niftynext50Value', changeId: 'niftynext50Change' },
            'nifty100': { valueId: 'nifty100Value', changeId: 'nifty100Change' }
        };

        const mapping = cardMap[indexKey];
        if (!mapping) return;

        const valueEl = document.getElementById(mapping.valueId);
        const changeEl = document.getElementById(mapping.changeId);

        if (!valueEl || !changeEl) return;

        // Update value
        valueEl.textContent = data.value.toLocaleString('en-IN', {
            minimumFractionDigits: 2,
            maximumFractionDigits: 2
        });

        // Update change
        const arrow = data.percentChange >= 0 ? '▲' : '▼';
        const sign = data.percentChange >= 0 ? '+' : '';
        changeEl.textContent = `${arrow} ${sign}${data.percentChange.toFixed(2)}%`;

        // Update colors
        const isUp = data.percentChange >= 0;

        if (isUp) {
            valueEl.classList.add('up');
            valueEl.classList.remove('down');
            changeEl.classList.add('up');
            changeEl.classList.remove('down');
        } else {
            valueEl.classList.add('down');
            valueEl.classList.remove('up');
            changeEl.classList.add('down');
            changeEl.classList.remove('up');
        }

        // Update card background
        const card = valueEl.closest('.hero-card');
        if (card) {
            if (isUp) {
                card.classList.remove('red');
            } else {
                card.classList.add('red');
            }
        }
    }

    updateIndexDetail(indexKey, data) {
        if (!data) return;

        // Update index name
        const indexNames = {
            'nifty50': 'NIFTY 50',
            'banknifty': 'BANK NIFTY',
            'sensex': 'SENSEX',
            'niftyfin': 'NIFTY FIN',
            'niftynext50': 'NIFTY NEXT 50',
            'nifty100': 'NIFTY 100'
        };

        const indexNameEl = document.getElementById('selectedIndexName');
        if (indexNameEl) {
            const arrow = indexNameEl.querySelector('.arrow');
            indexNameEl.innerHTML = `${indexNames[indexKey] || indexKey.toUpperCase()} `;
            if (arrow) {
                indexNameEl.appendChild(arrow);
            } else {
                indexNameEl.innerHTML += '<span class="arrow">›</span>';
            }
        }

        // Update main value
        const mainValueEl = document.getElementById('selectedIndexValue');
        if (mainValueEl) {
            mainValueEl.textContent = data.value.toLocaleString('en-IN', {
                minimumFractionDigits: 2,
                maximumFractionDigits: 2
            });
        }

        // Update change
        const mainChangeEl = document.getElementById('selectedIndexChange');
        if (mainChangeEl) {
            const sign = data.change >= 0 ? '+' : '';
            const percentSign = data.percentChange >= 0 ? '+' : '';
            mainChangeEl.textContent = `${sign}${data.change.toFixed(2)} (${percentSign}${data.percentChange.toFixed(2)}%)`;

            if (data.change >= 0) {
                mainChangeEl.classList.add('up');
                mainChangeEl.classList.remove('down');
            } else {
                mainChangeEl.classList.add('down');
                mainChangeEl.classList.remove('up');
            }
        }

        // Update Open/High/Low
        const openEl = document.getElementById('indexOpen');
        const highEl = document.getElementById('indexHigh');
        const lowEl = document.getElementById('indexLow');

        if (openEl) {
            openEl.textContent = data.open.toLocaleString('en-IN', {
                minimumFractionDigits: 2,
                maximumFractionDigits: 2
            });
        }
        if (highEl) {
            highEl.textContent = data.high.toLocaleString('en-IN', {
                minimumFractionDigits: 2,
                maximumFractionDigits: 2
            });
        }
        if (lowEl) {
            lowEl.textContent = data.low.toLocaleString('en-IN', {
                minimumFractionDigits: 2,
                maximumFractionDigits: 2
            });
        }
    }

    updateChart(indexKey, data) {
        if (!data || !data.history) return;

        const history = data.history;
        if (history.length === 0) return;

        // Get the chart from marketCharts global
        if (typeof marketCharts === 'undefined' || !marketCharts['marketIndexChart']) {
            return;
        }

        const chart = marketCharts['marketIndexChart'];

        // Update chart data
        const values = history.map(item => item.value);
        const labels = history.map(item => {
            // Extract time from timestamp (format: "YYYY-MM-DD HH:MM:SS")
            const parts = item.timestamp.split(' ');
            if (parts.length >= 2) {
                const timeParts = parts[1].split(':');
                return `${timeParts[0]}:${timeParts[1]}`;
            }
            return '';
        });

        chart.data.labels = labels;
        chart.data.datasets[0].data = values;

        // Update colors based on trend
        const isPositive = data.percentChange >= 0;
        const canvas = chart.canvas;
        const ctx = canvas.getContext('2d');
        const gradient = ctx.createLinearGradient(0, 0, 0, canvas.height);

        if (isPositive) {
            gradient.addColorStop(0, 'rgba(0, 200, 83, 0.3)');
            gradient.addColorStop(1, 'rgba(0, 200, 83, 0.01)');
            chart.data.datasets[0].borderColor = '#00c853';
            chart.data.datasets[0].backgroundColor = gradient;
        } else {
            gradient.addColorStop(0, 'rgba(255, 107, 107, 0.3)');
            gradient.addColorStop(1, 'rgba(255, 107, 107, 0.01)');
            chart.data.datasets[0].borderColor = '#ff6b6b';
            chart.data.datasets[0].backgroundColor = gradient;
        }

        chart.update('none');
    }

    setupCardClickHandlers() {
        const cardMap = {
            'nifty50Value': 'nifty50',
            'bankniftyValue': 'banknifty',
            'sensexValue': 'sensex',
            'niftyfinValue': 'niftyfin',
            'niftynext50Value': 'niftynext50',
            'nifty100Value': 'nifty100'
        };

        Object.keys(cardMap).forEach(valueId => {
            const element = document.getElementById(valueId);
            if (!element) return;

            const card = element.closest('.hero-card');
            if (!card) return;

            card.style.cursor = 'pointer';

            card.addEventListener('click', () => {
                const indexKey = cardMap[valueId];
                this.selectedIndex = indexKey;

                // Update detail and chart with latest data
                if (this.latestData && this.latestData[indexKey]) {
                    this.updateIndexDetail(indexKey, this.latestData[indexKey]);
                    this.updateChart(indexKey, this.latestData[indexKey]);
                }
            });
        });
    }

    async update() {
        const data = await this.fetchData();

        // Continue updating even if fetch fails
        if (!data || !data.success || !data.indices) {
            return;
        }

        const indices = data.indices;

        // If no indices data, don't update
        if (Object.keys(indices).length === 0) {
            return;
        }

        this.latestData = indices;

        // Update all index cards
        Object.keys(indices).forEach(key => {
            this.updateIndexCard(key, indices[key]);
        });

        // Update detail section and chart for selected index
        if (indices[this.selectedIndex]) {
            this.updateIndexDetail(this.selectedIndex, indices[this.selectedIndex]);
            this.updateChart(this.selectedIndex, indices[this.selectedIndex]);
        }

        // Mark as initialized after first successful update
        if (!this.initialized) {
            this.initialized = true;
        }
    }

    start() {
        if (this.isRunning) return;

        this.isRunning = true;
        console.log('Live indices updater started');

        // Setup click handlers
        this.setupCardClickHandlers();

        // First update immediately
        this.update();

        // Then update periodically
        this.intervalId = setInterval(() => {
            this.update();
        }, this.updateInterval);
    }

    stop() {
        if (!this.isRunning) return;

        this.isRunning = false;
        if (this.intervalId) {
            clearInterval(this.intervalId);
            this.intervalId = null;
        }
        console.log('Live indices updater stopped');
    }
}

// Initialize on page load
let liveUpdater = null;

document.addEventListener('DOMContentLoaded', () => {
    // Wait a bit for charts to initialize
    setTimeout(() => {
        liveUpdater = new LiveIndicesUpdater();
        liveUpdater.start();

        // Stop updates when page is hidden (save bandwidth)
        document.addEventListener('visibilitychange', () => {
            if (document.hidden) {
                liveUpdater.stop();
            } else {
                liveUpdater.start();
            }
        });
    }, 1000);
});
