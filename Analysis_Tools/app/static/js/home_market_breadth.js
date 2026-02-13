/* ===================================================================
   LIVE MARKET BREADTH UPDATER
   Fetches NSE advance-decline data during market hours only
   Market Hours: Monday-Friday, 9:15 AM - 3:30 PM IST
   =================================================================== */

let marketBreadthInterval = null;
let lastUpdateTime = null;

/**
 * Check if current time is within market hours
 * Market Hours: Monday-Friday, 9:15 AM - 3:30 PM IST
 */
function isMarketHours() {
  const now = new Date();

  // Get IST time
  const istTime = new Date(now.toLocaleString("en-US", { timeZone: "Asia/Kolkata" }));

  const day = istTime.getDay(); // 0 = Sunday, 6 = Saturday
  const hours = istTime.getHours();
  const minutes = istTime.getMinutes();

  // Check if weekday (Monday = 1, Friday = 5)
  if (day === 0 || day === 6) {
    console.log('[Market Breadth] Market closed - Weekend');
    return false;
  }

  // Convert current time to minutes since midnight
  const currentMinutes = hours * 60 + minutes;

  // Market hours: 9:15 AM = 555 minutes, 3:30 PM = 930 minutes
  const marketOpen = 9 * 60 + 15;   // 9:15 AM
  const marketClose = 15 * 60 + 30; // 3:30 PM

  const isOpen = currentMinutes >= marketOpen && currentMinutes <= marketClose;

  if (!isOpen) {
    console.log(`[Market Breadth] Market closed - Current time: ${hours}:${minutes}`);
  }

  return isOpen;
}

/**
 * Fetch and update market breadth data
 */
async function updateMarketBreadth() {
  // Only fetch live data during market hours
  // When market is closed, keep showing the EOD data from template
  if (!isMarketHours()) {
    console.log('[Market Breadth] Market closed - Showing EOD data from template');
    return;
  }

  try {
    console.log('[Market Breadth] Fetching live data from NSE...');

    const response = await fetch('/api/advance-decline');
    const data = await response.json();

    if (data.success) {
      const advances = data.advances || 0;
      const declines = data.declines || 0;
      const total = advances + declines;
      const timestamp = data.timestamp || '';

      console.log(`[Market Breadth] ✅ Advances: ${advances}, Declines: ${declines}, Time: ${timestamp}`);

      // Update the UI
      updateMarketBreadthUI(advances, declines, total, timestamp);

      // Update last update time
      lastUpdateTime = new Date();

    } else {
      console.warn('[Market Breadth] ⚠️ API returned error:', data.error);
      // Keep showing existing data, don't reset to zero
    }

  } catch (error) {
    console.error('[Market Breadth] ❌ Error fetching data:', error);
    // Keep showing existing data, don't reset to zero
  }
}

/**
 * Update the Market Breadth UI elements
 */
function updateMarketBreadthUI(advances, declines, total, timestamp) {
  // Calculate percentages
  const advancesPct = total > 0 ? (advances / total) * 100 : 50;
  const declinesPct = total > 0 ? (declines / total) * 100 : 50;

  // Find the breadth bar elements
  const advanceBar = document.querySelector('.breadth-bar .advance');
  const declineBar = document.querySelector('.breadth-bar .decline');

  if (advanceBar && declineBar) {
    // Update widths
    advanceBar.style.width = `${advancesPct}%`;
    declineBar.style.width = `${declinesPct}%`;

    // Update numbers inside bars
    const advanceSpan = advanceBar.querySelector('span');
    const declineSpan = declineBar.querySelector('span');

    if (advanceSpan) {
      advanceSpan.textContent = advances.toLocaleString('en-IN');
    }
    if (declineSpan) {
      declineSpan.textContent = declines.toLocaleString('en-IN');
    }

    // Add smooth transition effect
    advanceBar.style.transition = 'width 0.5s ease';
    declineBar.style.transition = 'width 0.5s ease';
  }

  // Update badge with timestamp
  const badge = document.querySelector('.insight-card.market-mood .insight-badge');
  if (badge) {
    if (timestamp && timestamp.trim() !== '') {
      // Display full timestamp: "12-Feb-2026 11:21:56"
      const formattedTime = formatTimestamp(timestamp);
      badge.textContent = formattedTime;
      badge.style.background = '#10b981';
      badge.style.color = 'white';
      badge.style.fontSize = '10px';
      badge.style.padding = '4px 8px';
      badge.style.whiteSpace = 'nowrap';
      badge.style.fontWeight = '500';
    } else {
      // No timestamp available - show "Live" during market hours
      if (isMarketHours()) {
        badge.textContent = 'Live';
        badge.style.background = '#10b981';
        badge.style.color = 'white';
        badge.style.fontSize = '11px';
        badge.style.padding = '4px 8px';
      }
    }
  }
}

/**
 * Format timestamp - return as-is from NSE
 * Input: "12-Feb-2026 11:21:56"
 * Output: "12-Feb-2026 11:21:56"
 */
function formatTimestamp(timestamp) {
  // Return the full timestamp as-is
  return timestamp;
}

/**
 * Start the market breadth live updater
 */
function startMarketBreadthUpdater() {
  console.log('=== MARKET BREADTH LIVE UPDATER INITIALIZED ===');

  // Initial update
  updateMarketBreadth();

  // Update every 30 seconds during market hours
  marketBreadthInterval = setInterval(() => {
    updateMarketBreadth();
  }, 30000); // 30 seconds

  console.log('[Market Breadth] Auto-update started (every 30 seconds)');
}

/**
 * Stop the market breadth updater
 */
function stopMarketBreadthUpdater() {
  if (marketBreadthInterval) {
    clearInterval(marketBreadthInterval);
    marketBreadthInterval = null;
    console.log('[Market Breadth] Auto-update stopped');
  }
}

/**
 * Initialize when DOM is ready
 */
if (document.readyState === 'loading') {
  document.addEventListener('DOMContentLoaded', startMarketBreadthUpdater);
} else {
  startMarketBreadthUpdater();
}

// Cleanup on page unload
window.addEventListener('beforeunload', stopMarketBreadthUpdater);
