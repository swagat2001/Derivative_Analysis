// ==============================
// 📊 STOCK DETAIL GAUGES SCRIPT
// ==============================

// --- Configuration for Gauge Styling ---
const gaugeOptions = (label, color, maxVal) => ({
  chart: {
    type: "radialBar",
    height: 140,
    sparkline: { enabled: true }
  },
  plotOptions: {
    radialBar: {
      hollow: { size: "55%" },
      track: { background: "#f1f5f9" },
      dataLabels: {
        name: {
          show: false
        },
        value: {
          show: true,
          fontSize: "16px",
          fontWeight: 700,
          color: color,
          offsetY: 6,
          formatter: function (val) {
            return parseFloat(val).toFixed(2);
          }
        }
      }
    }
  },
  colors: [color],
  labels: [label],
  series: [0], // Initial empty value
  stroke: { lineCap: "round" },
  fill: {
    type: "gradient",
    gradient: {
      shade: "light",
      type: "horizontal",
      gradientToColors: [color],
      stops: [0, 100]
    }
  },
  yaxis: { min: 0, max: maxVal }
});

// --- Create ApexCharts instances ---
const pcrGaugeChart = new ApexCharts(document.querySelector("#pcrGauge"), gaugeOptions("PCR", "#2563EB", 5));
const ivGaugeChart = new ApexCharts(document.querySelector("#ivGauge"), gaugeOptions("IV", "#10B981", 100));

// Render Gauges
pcrGaugeChart.render();
ivGaugeChart.render();

// --- Function to Update Gauge Values ---
async function updateGauges(ticker, date) {
  try {
    // Adjust API endpoint as per your backend
    const response = await fetch(`/api/stock_metrics?ticker=${ticker}&date=${date}`);
    const data = await response.json();

    if (!data.success) throw new Error("Invalid data");

    // --- Update PCR Gauge ---
    const pcrValue = parseFloat(data.pcr_oi || 0);
    pcrGaugeChart.updateSeries([pcrValue]);

    // --- Update IV Gauge ---
    const ivValue = parseFloat(data.avg_iv || 0);
    ivGaugeChart.updateSeries([ivValue]);

    // --- Update Key Metrics ---
    document.getElementById("underlyingPrice").textContent = data.underlying_price.toFixed(2);
    document.getElementById("oiChange").textContent = data.oi_change_pct.toFixed(2) + "%";
    document.getElementById("volChange").textContent = data.volume_change_pct.toFixed(2) + "%";
    document.getElementById("rsiValue").textContent = data.rsi.toFixed(2);

  } catch (err) {
    console.error("Gauge update failed:", err);
  }
}

// --- Auto Initialization ---
// This can be triggered when the stock detail page loads
document.addEventListener("DOMContentLoaded", () => {
  const ticker = document.body.getAttribute("data-ticker");
  const date = document.body.getAttribute("data-date");
  if (ticker && date) {
    updateGauges(ticker, date);
  }
});
