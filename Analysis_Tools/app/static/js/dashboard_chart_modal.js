// ============================ 
// DASHBOARD CHART MODAL JS
// ============================

let currentChart = null;

function openChartModal(element) {
  const stock = element.getAttribute('data-stock');
  const metric = element.getAttribute('data-metric');
  
  const modal = document.getElementById('chartModal');
  const title = document.getElementById('modalChartTitle');
  const loading = document.getElementById('chartLoading');
  const canvas = document.getElementById('chartCanvas');
  
  // Show modal
  modal.style.display = 'block';
  loading.style.display = 'flex';
  canvas.style.display = 'none';
  
  // Update title
  const metricName = metric === 'call_money' ? 'Call Money' : 'Put Money';
  title.textContent = `${stock} - ${metricName} (Last 30 Days)`;
  
  // Fetch and render chart
  fetch(`/api/chart-data/${stock}/${metric}`)
    .then(response => response.json())
    .then(result => {
      if (result.success && result.data.length > 0) {
        renderChart(result.data, metricName);
      } else {
        canvas.innerHTML = '<div style="display: flex; justify-content: center; align-items: center; height: 100%; color: #6b7280;">No data available</div>';
        canvas.style.display = 'block';
      }
      loading.style.display = 'none';
    })
    .catch(error => {
      console.error('Error fetching chart data:', error);
      canvas.innerHTML = '<div style="display: flex; justify-content: center; align-items: center; height: 100%; color: #ef4444;">Error loading chart</div>';
      canvas.style.display = 'block';
      loading.style.display = 'none';
    });
}

function renderChart(data, metricName) {
  const canvas = document.getElementById('chartCanvas');
  const legend = document.getElementById('chartLegend');
  
  canvas.style.display = 'block';
  canvas.innerHTML = '';
  
  // Create chart
  const chart = LightweightCharts.createChart(canvas, {
    layout: {
      background: { color: '#ffffff' },
      textColor: '#333',
    },
    grid: {
      vertLines: { color: '#e5e7eb' },
      horzLines: { color: '#e5e7eb' },
    },
    width: canvas.clientWidth,
    height: 400,
    timeScale: {
      timeVisible: true,
      secondsVisible: false,
    },
  });
  
  // Add line series
  const lineSeries = chart.addLineSeries({
    color: '#2563eb',
    lineWidth: 2,
  });
  
  // Format data for chart
  const chartData = data.map(point => ({
    time: point.date,
    value: point.value
  })).sort((a, b) => new Date(a.time) - new Date(b.time));
  
  lineSeries.setData(chartData);
  chart.timeScale().fitContent();
  
  // Update legend
  legend.innerHTML = `
    <div class="legend-item">
      <div class="legend-color" style="background-color: #2563eb;"></div>
      <span>${metricName}</span>
    </div>
    <div class="legend-item">
      <span style="color: #6b7280;">Points: ${chartData.length}</span>
    </div>
  `;
  
  currentChart = chart;
  
  // Handle resize
  window.addEventListener('resize', () => {
    if (currentChart) {
      currentChart.applyOptions({ width: canvas.clientWidth });
    }
  });
}

function closeChartModal() {
  const modal = document.getElementById('chartModal');
  modal.style.display = 'none';
  
  if (currentChart) {
    currentChart.remove();
    currentChart = null;
  }
}

// Close modal when clicking outside
window.onclick = function(event) {
  const modal = document.getElementById('chartModal');
  if (event.target === modal) {
    closeChartModal();
  }
};

// Close on ESC key
document.addEventListener('keydown', function(event) {
  if (event.key === 'Escape') {
    closeChartModal();
  }
});