/**
 * Home Page Live FII/DII Updates
 * Fetches and updates FII/DII data periodically
 *
 * Location: static/js/home_fii_dii_updates.js
 */

(function() {
    'use strict';

    // Update interval (5 minutes = 300000ms)
    const FII_DII_UPDATE_INTERVAL = 300000;

    /**
     * Format number in Indian currency format (Crores)
     */
    function formatCrores(value) {
        const absValue = Math.abs(value);
        const sign = value >= 0 ? '+' : '-';
        return `${sign}₹${absValue.toLocaleString('en-IN', { maximumFractionDigits: 0 })} Cr`;
    }

    /**
     * Update FII/DII display
     */
    function updateFiiDiiDisplay(data) {
        const fiiNetElement = document.getElementById('fiiNetValue');
        const diiNetElement = document.getElementById('diiNetValue');

        if (fiiNetElement && data.fii_net !== undefined) {
            const fiiValue = parseFloat(data.fii_net);
            fiiNetElement.textContent = formatCrores(fiiValue);
            fiiNetElement.className = 'activity-value ' + (fiiValue >= 0 ? 'up' : 'down');
        }

        if (diiNetElement && data.dii_net !== undefined) {
            const diiValue = parseFloat(data.dii_net);
            diiNetElement.textContent = formatCrores(diiValue);
            diiNetElement.className = 'activity-value ' + (diiValue >= 0 ? 'up' : 'down');
        }

        console.log('[FII/DII] Updated:', {
            fii_net: data.fii_net,
            dii_net: data.dii_net,
            source: data.source
        });
    }

    /**
     * Fetch FII/DII data from API
     */
    async function fetchFiiDiiData() {
        try {
            const response = await fetch('/api/live-fii-dii');

            if (!response.ok) {
                console.warn('[FII/DII] API returned status:', response.status);
                return null;
            }

            const data = await response.json();

            if (data.success) {
                updateFiiDiiDisplay(data);
                return data;
            } else {
                console.warn('[FII/DII] API returned error:', data.message);
                return null;
            }

        } catch (error) {
            console.error('[FII/DII] Fetch error:', error);
            return null;
        }
    }

    /**
     * Initialize FII/DII updates
     */
    function initFiiDiiUpdates() {
        // Only initialize if FII/DII elements exist on page
        if (!document.getElementById('fiiNetValue')) {
            return;
        }

        console.log('[FII/DII] Initializing live updates...');

        // Set up periodic updates
        setInterval(fetchFiiDiiData, FII_DII_UPDATE_INTERVAL);

        console.log('[FII/DII] Update interval:', FII_DII_UPDATE_INTERVAL / 1000, 'seconds');
    }

    // Initialize when DOM is ready
    if (document.readyState === 'loading') {
        document.addEventListener('DOMContentLoaded', initFiiDiiUpdates);
    } else {
        initFiiDiiUpdates();
    }

})();
