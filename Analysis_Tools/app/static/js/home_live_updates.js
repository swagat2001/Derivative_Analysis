/**
 * Live Indices Data Updates
 *
 * Data source priority:
 *   1. NSE API  (/api/nse-indices)      → always-on price cards  (every 5 s)
 *   2. NSE API  (/api/nse-chart/<idx>)  → always-on chart series (on load + click + every 60 s)
 *   3. Upstox   (/api/live-indices)     → tick-precision override ONLY when today's data exists (every 1 s)
 *
 * Key rule: Upstox history files persist from the previous session, so we
 * only use Upstox chart data if its latest history timestamp is from TODAY.
 */

// ─── Helpers ──────────────────────────────────────────────────────────────────

function todayDateStr() {
    const d = new Date();
    const yy = d.getFullYear();
    const mm = String(d.getMonth() + 1).padStart(2, '0');
    const dd = String(d.getDate()).padStart(2, '0');
    return `${yy}-${mm}-${dd}`;
}

/**
 * Returns true only when the Upstox history contains data from today.
 * history items have: { timestamp: "YYYY-MM-DD HH:MM:SS", value: float }
 */
function upstoxHistoryIsToday(history) {
    if (!history || history.length === 0) return false;
    const lastItem = history[history.length - 1];
    if (!lastItem || !lastItem.timestamp) return false;
    return lastItem.timestamp.startsWith(todayDateStr());
}

/**
 * Safe gradient helper – avoids broken gradient on zero-height canvas.
 */
function makeGradient(ctx, canvas, isPositive) {
    const h = canvas.offsetHeight || canvas.height || 300;
    const grad = ctx.createLinearGradient(0, 0, 0, h);
    if (isPositive) {
        grad.addColorStop(0, 'rgba(0, 200, 83, 0.3)');
        grad.addColorStop(1, 'rgba(0, 200, 83, 0.01)');
        return { borderColor: '#00c853', backgroundColor: grad };
    } else {
        grad.addColorStop(0, 'rgba(255, 107, 107, 0.3)');
        grad.addColorStop(1, 'rgba(255, 107, 107, 0.01)');
        return { borderColor: '#ff6b6b', backgroundColor: grad };
    }
}

// ─── Main class ───────────────────────────────────────────────────────────────

class LiveIndicesUpdater {
    constructor() {
        this.upstoxInterval = 1000;   // ms
        this.nsePriceInterval = 5000;   // ms
        this.nseChartInterval = 60000;  // ms

        this._upstoxTimer = null;
        this._nsePriceTimer = null;
        this._nseChartTimer = null;

        this.isRunning = false;
        this.selectedIndex = 'nifty50';
        this.latestUpstoxData = null;
        this.latestNseData = null;
    }

    // ─── DOM helpers ──────────────────────────────────────────────────────────

    _cardMap() {
        return {
            'nifty50': { valueId: 'nifty50Value', changeId: 'nifty50Change' },
            'banknifty': { valueId: 'bankniftyValue', changeId: 'bankniftyChange' },
            'sensex': { valueId: 'sensexValue', changeId: 'sensexChange' },
            'niftyfin': { valueId: 'niftyfinValue', changeId: 'niftyfinChange' },
            'niftynext50': { valueId: 'niftynext50Value', changeId: 'niftynext50Change' },
            'nifty100': { valueId: 'nifty100Value', changeId: 'nifty100Change' },
            'indiavix': { valueId: 'indiavixValue', changeId: 'indiavixChange' },
        };
    }

    updateIndexCard(indexKey, data) {
        const mapping = this._cardMap()[indexKey];
        if (!mapping) return;
        const valueEl = document.getElementById(mapping.valueId);
        const changeEl = document.getElementById(mapping.changeId);
        if (!valueEl || !changeEl) return;

        valueEl.textContent = (data.value || 0).toLocaleString('en-IN', {
            minimumFractionDigits: 2, maximumFractionDigits: 2
        });

        const pct = data.percentChange ?? 0;
        const arrow = pct >= 0 ? '▲' : '▼';
        const sign = pct >= 0 ? '+' : '';
        changeEl.textContent = `${arrow} ${sign}${pct.toFixed(2)}%`;

        const isUp = pct >= 0;
        valueEl.classList.toggle('up', isUp);
        valueEl.classList.toggle('down', !isUp);
        changeEl.classList.toggle('up', isUp);
        changeEl.classList.toggle('down', !isUp);

        const card = valueEl.closest('.hero-card');
        if (card) card.classList.toggle('red', !isUp);
    }

    updateIndexDetail(indexKey, data) {
        if (!data) return;

        const indexNames = {
            'nifty50': 'NIFTY 50', 'banknifty': 'BANK NIFTY',
            'sensex': 'SENSEX', 'niftyfin': 'NIFTY FIN',
            'niftynext50': 'NIFTY NEXT 50', 'nifty100': 'NIFTY 100',
            'indiavix': 'INDIA VIX',
        };

        const nameEl = document.getElementById('selectedIndexName');
        if (nameEl) nameEl.innerHTML = `${indexNames[indexKey] || indexKey.toUpperCase()} <span class="arrow">›</span>`;

        const mainVal = document.getElementById('selectedIndexValue');
        if (mainVal) mainVal.textContent = (data.value || 0).toLocaleString('en-IN', {
            minimumFractionDigits: 2, maximumFractionDigits: 2
        });

        const mainChg = document.getElementById('selectedIndexChange');
        if (mainChg) {
            const chg = data.change ?? 0;
            const pct = data.percentChange ?? data.percent ?? 0;
            mainChg.textContent = `${chg >= 0 ? '+' : ''}${chg.toFixed(2)} (${pct >= 0 ? '+' : ''}${pct.toFixed(2)}%)`;
            mainChg.classList.toggle('up', chg >= 0);
            mainChg.classList.toggle('down', chg < 0);
        }

        const fmt = v => (v || 0).toLocaleString('en-IN', { minimumFractionDigits: 2, maximumFractionDigits: 2 });
        const openEl = document.getElementById('indexOpen');
        const highEl = document.getElementById('indexHigh');
        const lowEl = document.getElementById('indexLow');
        if (openEl) openEl.textContent = fmt(data.open);
        if (highEl) highEl.textContent = fmt(data.high);
        if (lowEl) lowEl.textContent = fmt(data.low);
    }

    // ─── Chart update ─────────────────────────────────────────────────────────

    /**
     * Update the main Chart.js instance.
     *
     * Accepts two formats:
     *   NSE:    { series: [[epoch_ms, price], ...], percent, open, high, low }
     *   Upstox: { history: [{timestamp: "YYYY-MM-DD HH:MM:SS", value}], percentChange }
     */
    updateChart(indexKey, data) {
        if (typeof marketCharts === 'undefined') return;
        const chart = marketCharts['marketIndexChart'];
        if (!chart) return;

        let values = [], labels = [];

        // ── NSE epoch-ms series ────────────────────────────────────────────────
        if (data.series && data.series.length > 0) {
            for (const [ts_ms, price] of data.series) {
                const d = new Date(ts_ms);
                const hh = d.getHours().toString().padStart(2, '0');
                const mm = d.getMinutes().toString().padStart(2, '0');
                labels.push(`${hh}:${mm}`);
                values.push(price);
            }
        }
        // ── Upstox timestamp string history ───────────────────────────────────
        else if (data.history && data.history.length > 0) {
            for (const item of data.history) {
                const parts = (item.timestamp || '').split(' ');
                labels.push(parts.length >= 2 ? parts[1].substring(0, 5) : '');
                values.push(item.value);
            }
        }

        if (values.length === 0) return;

        // Determine trend
        const rawPct = data.percentChange ?? data.percent ?? (values[values.length - 1] - values[0]);
        const isPositive = rawPct >= 0;

        const canvas = chart.canvas;
        const ctx = canvas.getContext('2d');
        const colors = makeGradient(ctx, canvas, isPositive);

        chart.data.labels = labels;
        chart.data.datasets[0].data = values;
        chart.data.datasets[0].borderColor = colors.borderColor;
        chart.data.datasets[0].backgroundColor = colors.backgroundColor;

        chart.update('none');
    }

    // ─── Fetch helpers ────────────────────────────────────────────────────────

    async fetchNseChart(indexKey) {
        try {
            const resp = await fetch(`/api/nse-chart/${indexKey}`);
            if (!resp.ok) return null;
            const json = await resp.json();
            return json.success ? json : null;
        } catch { return null; }
    }

    async fetchNsePrices() {
        try {
            const resp = await fetch('/api/nse-indices');
            if (!resp.ok) return null;
            const json = await resp.json();
            return json.success && Object.keys(json.indices || {}).length > 0 ? json.indices : null;
        } catch { return null; }
    }

    async fetchUpstoxData() {
        try {
            const resp = await fetch('/api/live-indices');
            if (!resp.ok) return null;
            const json = await resp.json();
            if (!json.success || !json.indices) return null;
            if (Object.keys(json.indices).length === 0) return null;
            return json.indices;
        } catch { return null; }
    }

    // ─── Poll loops ───────────────────────────────────────────────────────────

    async _pollNsePrices() {
        const data = await this.fetchNsePrices();
        if (!data) return;
        this.latestNseData = data;

        for (const key of Object.keys(data)) {
            // Only update card from NSE if Upstox hasn't provided today's data for this key
            const upstoxOk = this.latestUpstoxData
                && this.latestUpstoxData[key]
                && upstoxHistoryIsToday(this.latestUpstoxData[key].history);
            if (!upstoxOk) {
                this.updateIndexCard(key, data[key]);
            }
        }

        // Update detail section for selected index if Upstox not providing today's data
        const selUpstox = this.latestUpstoxData && this.latestUpstoxData[this.selectedIndex];
        if (!selUpstox || !upstoxHistoryIsToday(selUpstox.history)) {
            if (data[this.selectedIndex]) {
                this.updateIndexDetail(this.selectedIndex, data[this.selectedIndex]);
            }
        }
    }

    async _pollNseChart() {
        // Skip if Upstox already providing today's chart
        const selUpstox = this.latestUpstoxData && this.latestUpstoxData[this.selectedIndex];
        if (selUpstox && upstoxHistoryIsToday(selUpstox.history)) return;

        const data = await this.fetchNseChart(this.selectedIndex);
        if (data) this.updateChart(this.selectedIndex, data);
    }

    async _pollUpstox() {
        const data = await this.fetchUpstoxData();
        if (!data) return;
        this.latestUpstoxData = data;

        const sel = data[this.selectedIndex];
        if (!sel) return;

        // Only use Upstox data if it's from TODAY's session
        if (upstoxHistoryIsToday(sel.history)) {
            // Override NSE with live tick data
            for (const key of Object.keys(data)) {
                this.updateIndexCard(key, data[key]);
            }
            this.updateIndexDetail(this.selectedIndex, sel);
            this.updateChart(this.selectedIndex, sel);
        }
        // (if not today → NSE is primary, do nothing here)
    }

    // ─── Card click handlers ──────────────────────────────────────────────────

    setupCardClickHandlers() {
        const cardMap = {
            'nifty50Value': 'nifty50', 'bankniftyValue': 'banknifty',
            'sensexValue': 'sensex', 'niftyfinValue': 'niftyfin',
            'niftynext50Value': 'niftynext50', 'nifty100Value': 'nifty100',
            'indiavixValue': 'indiavix',
        };

        for (const [valueId, indexKey] of Object.entries(cardMap)) {
            const el = document.getElementById(valueId);
            const card = el && el.closest('.hero-card');
            if (!card) continue;
            card.style.cursor = 'pointer';

            card.addEventListener('click', async () => {
                this.selectedIndex = indexKey;
                document.querySelectorAll('.hero-card').forEach(c => c.classList.remove('active'));
                card.classList.add('active');

                // Update detail from best available source
                const selUpstox = this.latestUpstoxData && this.latestUpstoxData[indexKey];
                const hasToday = selUpstox && upstoxHistoryIsToday(selUpstox.history);
                const best = hasToday ? selUpstox : (this.latestNseData && this.latestNseData[indexKey]);
                if (best) this.updateIndexDetail(indexKey, best);

                // Load chart: use Upstox if today, else NSE
                if (hasToday) {
                    this.updateChart(indexKey, selUpstox);
                } else {
                    const chartData = await this.fetchNseChart(indexKey);
                    if (chartData) this.updateChart(indexKey, chartData);
                }
            });
        }
    }

    // ─── Lifecycle ────────────────────────────────────────────────────────────

    async start() {
        if (this.isRunning) return;
        this.isRunning = true;
        console.log('[LiveUpdater] Started – today is', todayDateStr());

        this.setupCardClickHandlers();

        // ── Immediate first loads ─────────────────────────────────────────────

        // 1. NSE chart immediately replaces dummy data
        const initChart = await this.fetchNseChart(this.selectedIndex);
        if (initChart) {
            console.log('[LiveUpdater] NSE chart loaded –', (initChart.series || []).length, 'points');
            this.updateChart(this.selectedIndex, initChart);
        }

        // 2. NSE prices fill cards
        await this._pollNsePrices();

        // 3. Upstox override only if today's data exists
        await this._pollUpstox();

        // ── Recurring polls ───────────────────────────────────────────────────
        this._nsePriceTimer = setInterval(() => this._pollNsePrices(), this.nsePriceInterval);
        this._nseChartTimer = setInterval(() => this._pollNseChart(), this.nseChartInterval);
        this._upstoxTimer = setInterval(() => this._pollUpstox(), this.upstoxInterval);
    }

    stop() {
        if (!this.isRunning) return;
        this.isRunning = false;
        clearInterval(this._upstoxTimer);
        clearInterval(this._nsePriceTimer);
        clearInterval(this._nseChartTimer);
        this._upstoxTimer = this._nsePriceTimer = this._nseChartTimer = null;
        console.log('[LiveUpdater] Stopped');
    }
}

// ── Boot ──────────────────────────────────────────────────────────────────────

let liveUpdater = null;

document.addEventListener('DOMContentLoaded', () => {
    // Wait 800ms for home_market_charts.js to init the canvas first
    setTimeout(async () => {
        liveUpdater = new LiveIndicesUpdater();
        await liveUpdater.start();

        document.addEventListener('visibilitychange', () => {
            if (document.hidden) {
                liveUpdater.stop();
            } else {
                liveUpdater.start();
            }
        });
    }, 800);
});
