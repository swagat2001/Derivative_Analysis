/**
 * Market Breadth Live Updates
 * Fetches and updates market breadth data every 2 minutes
 */

async function updateMarketBreadth() {
    try {
        const response = await fetch('/api/market-breadth');
        const data = await response.json();

        if (data && data.total > 0) {
            const advances = data.advances || 0;
            const declines = data.declines || 0;
            const total = advances + declines;

            // Calculate percentages
            const advPct = total > 0 ? (advances / total * 100) : 50;
            const decPct = total > 0 ? (declines / total * 100) : 50;

            // Update counts in spans
            const advanceCountEl = document.querySelector('#breadth-advance-bar span, .advance span');
            const declineCountEl = document.querySelector('#breadth-decline-bar span, .decline span');

            if (advanceCountEl) advanceCountEl.textContent = advances.toLocaleString();
            if (declineCountEl) declineCountEl.textContent = declines.toLocaleString();

            // Update bar widths
            const advanceBar = document.querySelector('#breadth-advance-bar, .advance');
            const declineBar = document.querySelector('#breadth-decline-bar, .decline');

            if (advanceBar) advanceBar.style.width = advPct + '%';
            if (declineBar) declineBar.style.width = decPct + '%';

            console.log('✅ Market breadth updated:', {advances, declines, total});
        }
    } catch (error) {
        console.error('❌ Market breadth update error:', error);
    }
}

// Initialize on page load
document.addEventListener('DOMContentLoaded', function() {
    // Initial update
    updateMarketBreadth();

    // Auto-refresh every 2 minutes (120000 ms)
    setInterval(updateMarketBreadth, 120000);

    console.log('✅ Market Breadth auto-update initialized');
});
