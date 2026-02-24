/* ===== GOLDMINE SCREENERS - TAB-BASED INTERFACE ===== */

let currentActiveTab = 'derivative';
let currentStocks = [];
let currentDataKey = '';

/* ===== TAB SWITCHING ===== */
function switchTab(tabName) {
  console.log('Switching to tab:', tabName);

  // Update Tab Data-Attributes
  document.querySelectorAll('.goldmine-tab').forEach(tab => tab.classList.remove('active'));
  const activeTabBtn = document.querySelector(`.goldmine-tab[data-tab="${tabName}"]`);
  if (activeTabBtn) activeTabBtn.classList.add('active');

  // Hide all list content
  document.querySelectorAll('.goldmine-tab-content').forEach(content => content.classList.remove('active'));

  // Show target list content
  const targetContent = document.getElementById(`tab-${tabName}`);
  if (targetContent) targetContent.classList.add('active');

  // Ensure we are in list view
  showScreenerList();

  currentActiveTab = tabName;
  renderCategoryScreeners(tabName);
}

/* ===== VIEW NAVIGATION ===== */
function showScreenerList() {
  document.getElementById('screenerListView').style.display = 'block';
  document.getElementById('detailView').style.display = 'none';
  window.scrollTo(0, 0);
}

function showDetailViewContainer() {
  document.getElementById('screenerListView').style.display = 'none';
  document.getElementById('detailView').style.display = 'block';
  window.scrollTo(0, 0);
}

/* ===== RENDER CATEGORY SCREENERS ===== */
function renderCategoryScreeners(tabName) {
  if (typeof SCREENER_DATA === 'undefined') {
    console.error('ERROR: SCREENER_DATA not defined!');
    return;
  }

  const categoryMap = {
    'derivative': 'Derivative Scanners',
    'technical': 'Technical Scanners',
    'fundamental': 'Fundamental Scanners',
    'intraday': 'Intraday Scanners',
    'price-volume': 'Price & Volume Scanners'
  };

  const category = categoryMap[tabName];
  console.log(`Rendering tab: ${tabName}, Category: ${category}`);

  const screeners = SCREENER_DATA[category];
  const listEl = document.getElementById(`screenerList-${tabName}`);

  if (!listEl) {
    console.error(`List element not found: screenerList-${tabName}`);
    return;
  }

  if (!screeners) {
    console.warn(`Category "${category}" not found in SCREENER_DATA. Available:`, Object.keys(SCREENER_DATA));
    listEl.innerHTML = `<div class="no-data" style="text-align:center; padding:30px; color:#6b7280;">Category "${category}" not found. Available: ${Object.keys(SCREENER_DATA).join(', ')}</div>`;
    return;
  }

  listEl.innerHTML = '';

  screeners.forEach(item => {
    const card = document.createElement('div');
    card.className = 'screener-card';
    card.style.cursor = 'pointer';

    let badgeHTML = '';
    if (item.badge) {
      const badgeClass = item.badge.toLowerCase().replace(' ', '-');
      badgeHTML = `<span class="screener-badge badge-${badgeClass}">${item.badge}</span>`;
    }

    card.innerHTML = `
      <div class="screener-left">
        <div class="icon">${item.icon}</div>
        <div class="screener-info">
          <div class="screener-title">${item.title} ${badgeHTML}</div>
          <div class="screener-desc">${item.description}</div>
        </div>
      </div>
      <div class="screener-right">
        <svg width="20" height="20" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2">
          <path d="M9 18l6-6-6-6"/>
        </svg>
      </div>
    `;

    // Handle click
    card.onclick = () => {
      // Logic for inline API fetching (Fundamental / Intraday)
      if (item.inline) {
        if (item.apiUrl) {
          fetchAndShowDetailView(item.dataKey, item.apiUrl);
        } else {
          console.warn("Inline item missing apiUrl:", item);
        }
      }
      // Logic for external URL navigation
      else if (item.url) {
        const separator = item.url.includes('?') ? '&' : '?';
        window.location.href = `${item.url}${separator}source=goldmine&tab=${currentActiveTab}`;
      }
    };

    listEl.appendChild(card);
  });
}

/* ===== DATA FETCHING & DETAIL VIEW ===== */
function fetchAndShowDetailView(dataKey, apiUrl) {
  // Show loading state, reusing detail view container
  showDetailViewContainer();
  const detailHeader = document.querySelector('.detail-header');
  const tableWrap = document.querySelector('.table-wrap');

  // Temporary loading state
  document.getElementById('stocksTableBody').innerHTML = '<tr><td colspan="8" style="text-align:center; padding: 40px;"><div class="loading-spinner"></div><p>Loading data...</p></td></tr>';

  fetch(apiUrl)
    .then(response => {
      if (!response.ok) throw new Error(`HTTP error! status: ${response.status}`);
      return response.json();
    })
    .then(data => {
      console.log('Data received:', data);

      // Store data
      SCREENER_DATA[dataKey] = {
        title: data.title,
        tag: data.tag,
        description: data.description,
        stocks: data.stocks || []
      };

      currentDataKey = dataKey;
      currentStocks = data.stocks || [];

      renderDetailView(dataKey);
    })
    .catch(error => {
      console.error('Error fetching data:', error);
      document.getElementById('stocksTableBody').innerHTML = `
        <tr><td colspan="8" style="text-align:center; padding: 40px;">
          <p style="color: #e74c3c;">Failed to load data</p>
          <p style="color: #666; font-size: 14px;">${error.message}</p>
          <button class="btn primary" onclick="showScreenerList()" style="margin-top: 20px;">Back to Scanners</button>
        </td></tr>
      `;
    });
}

function renderDetailView(dataKey) {
  const data = SCREENER_DATA[dataKey];
  if (!data) return;

  // Update Header
  document.getElementById('detailTitle').textContent = data.title;
  document.getElementById('detailDesc').textContent = data.description;

  // Render Table Code
  renderTable(data.stocks, dataKey);
}

/* ===== FILTER & SORT ===== */
function filterStocks(filter) {
  // Update active tag - find all tags and remove active, then add to clicked one
  // Note: Goldmine page might not have the filter tags UI yet?
  // If not, this function might be unused, but good to have for consistency if we add the UI later.
  // For now, let's keep it compatible.

  if (event && event.target) {
    document.querySelectorAll('.detail-tag').forEach(tag => tag.classList.remove('active'));
    event.target.classList.add('active');
  }

  let filtered = [...(SCREENER_DATA[currentDataKey]?.stocks || [])];

  if (filter === 'bullish') {
    filtered = filtered.filter(s => s.signal === 'BULLISH');
  } else if (filter === 'bearish') {
    filtered = filtered.filter(s => s.signal === 'BEARISH');
  } else if (filter === 'neutral') {
    filtered = filtered.filter(s => s.signal === 'NEUTRAL');
  }

  currentStocks = filtered;
  renderTable(filtered, currentDataKey);
}

let sortDirection = {};
function sortTable(column) {
  sortDirection[column] = !sortDirection[column];
  const dir = sortDirection[column] ? 1 : -1;

  currentStocks.sort((a, b) => {
    if (column === 'name') {
      return dir * a.ticker.localeCompare(b.ticker);
    }
    // Handle numeric sorts safely
    const valA = a[column] || 0;
    const valB = b[column] || 0;
    return dir * (valA - valB);
  });

  renderTable(currentStocks, currentDataKey);
}

function toggleSelectAll() {
  const selectAll = document.getElementById('selectAll');
  document.querySelectorAll('.stock-checkbox').forEach(cb => cb.checked = selectAll.checked);
}

function renderTable(stocks, dataKey) {
  const tableHead = document.querySelector('#stocksTable thead');
  const tbody = document.getElementById('stocksTableBody');

  if (!tableHead || !tbody) return;

  // 1. Render Fixed Headers (matching screener_landing.html)
  tableHead.innerHTML = `
    <tr>
      <th><input type="checkbox" id="selectAll" onclick="toggleSelectAll()"/></th>
      <th onclick="sortTable('name')" style="cursor:pointer">Name</th>
      <th onclick="sortTable('price')" style="cursor:pointer">Price</th>
      <th onclick="sortTable('market_cap')" style="cursor:pointer">Market Cap (Cr)</th>
      <th onclick="sortTable('pe')" style="cursor:pointer">P/E Ratio</th>
      <th onclick="sortTable('change')" style="cursor:pointer">Day Change</th>
      <th onclick="sortTable('change_pct')" style="cursor:pointer">Change %</th>
      <th onclick="sortTable('volume')" style="cursor:pointer">Volume</th>
      <th>Key Metric</th>
      <th>Signal</th>
    </tr>
  `;

  tbody.innerHTML = '';

  if (!stocks || stocks.length === 0) {
    tbody.innerHTML = '<tr><td colspan="10" style="text-align:center; padding: 20px;">No stocks found matching criteria.</td></tr>';
    if (document.getElementById('showingCount')) document.getElementById('showingCount').textContent = '0';
    if (document.getElementById('totalCount')) document.getElementById('totalCount').textContent = '0';
    return;
  }

  stocks.forEach(stock => {
    const row = document.createElement('tr');
    row.dataset.ticker = stock.ticker;

    const changeClass = stock.change > 0 ? 'positive' : (stock.change < 0 ? 'negative' : '');
    const changeSign = stock.change > 0 ? '+' : '';
    // Use signal-badge classes or fallback
    const signalClass = (stock.signal || 'NEUTRAL').toLowerCase();
    // Note: screener_landing uses css classes like .bullish, .bearish.
    // We need to ensure goldmine.html has these styles or they won't show colors.
    // I will use inline styles for safety if classes are missing in goldmine css?
    // Actually screener_landing.html uses 'signal-badge bull/bear/neutral'

    row.innerHTML = `
      <td><input type="checkbox" class="stock-checkbox" value="${stock.ticker}"/></td>
      <td class="stock-name">
        <span style="font-weight:600">${stock.ticker}</span>
        <span class="exchange" style="font-size:10px; color:#666; display:block">NSE</span>
      </td>
      <td>₹${formatNumber(stock.price, 2)}</td>
      <td>₹${formatNumber(stock.market_cap)} Cr</td>
      <td>${stock.pe ? formatNumber(stock.pe, 2) : '-'}</td>
      <td class="${changeClass}" style="${stock.change >= 0 ? 'color:#28a745' : 'color:#dc3545'}">${changeSign}${formatNumber(stock.change, 2)}</td>
      <td class="${changeClass}" style="${stock.change >= 0 ? 'color:#28a745' : 'color:#dc3545'}">${changeSign}${formatNumber(stock.change_pct, 2)}%</td>
      <td>${formatNumber(stock.volume)}</td>
      <td style="font-weight: 500; color: #3498db;">${stock.custom_metric_value || '-'}</td>
      <td><span class="signal-badge ${signalClass}">${stock.signal || 'NEUTRAL'}</span></td>
    `;

    // Double click to navigate
    row.ondblclick = () => {
      window.location.href = `/stock/${stock.ticker}`;
    };

    tbody.appendChild(row);
  });

  if (document.getElementById('showingCount')) document.getElementById('showingCount').textContent = stocks.length;
  // Use optional chaining for safety
  const total = SCREENER_DATA[dataKey]?.stocks?.length || stocks.length;
  if (document.getElementById('totalCount')) document.getElementById('totalCount').textContent = total;

  if (typeof initTableSorting === 'function') {
    setTimeout(initTableSorting, 50);
  }
}

/* ===== UTILS ===== */
function formatNumber(num, decimals = 0) {
  if (num === null || num === undefined) return '-';
  const n = Number(num);
  if (isNaN(n)) return num; // Return as is if not a number
  return n.toLocaleString('en-IN', {
    minimumFractionDigits: decimals,
    maximumFractionDigits: decimals
  });
}

function exportToCSV() {
  if (!currentStocks || currentStocks.length === 0) return;

  // Simple CSV export
  // Get all keys from first object as headers
  const headers = Object.keys(currentStocks[0]);
  const csvContent = "data:text/csv;charset=utf-8,"
    + headers.join(",") + "\n"
    + currentStocks.map(row => headers.map(fieldName => {
      let val = row[fieldName];
      if (typeof val === 'string' && val.includes(',')) val = `"${val}"`; // Escape commas
      return val;
    }).join(",")).join("\n");

  const encodedUri = encodeURI(csvContent);
  const link = document.createElement("a");
  link.setAttribute("href", encodedUri);
  link.setAttribute("download", `goldmine_screener_${currentDataKey || 'export'}.csv`);
  document.body.appendChild(link);
  link.click();
  document.body.removeChild(link);
}

/* ===== INITIALIZATION ===== */
if (document.readyState === 'loading') {
  document.addEventListener('DOMContentLoaded', initGoldmine);
} else {
  initGoldmine();
}

function initGoldmine() {
  console.log('=== INITIALIZING GOLDMINE ===');
  const urlParams = new URLSearchParams(window.location.search);
  const tab = urlParams.get('tab');
  if (tab) {
    switchTab(tab);
  } else {
    switchTab('derivative');
  }
}
