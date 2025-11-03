/* ==========================================================
   📊 STOCK DETAIL OI CHARTS — TradingView Layout
   ========================================================== */

document.addEventListener("DOMContentLoaded", function () {
  const container = document.getElementById("oi-chart-container");
  if (!container) return;

  let chartData;
  try {
    chartData = JSON.parse(container.dataset.chart || "{}");
  } catch (err) {
    console.error("❌ Failed to parse chart data:", err);
    return;
  }

  if (!chartData || !chartData.strikes) {
    console.warn("⚠️ No chart data found or invalid structure.");
    return;
  }

  const ticker = chartData.meta?.ticker || "Unknown";
  const expiry = chartData.meta?.expiry || "Unknown";
  const strikes = chartData.strikes;

  // Helper to format large numbers (e.g. 25.3L, 10.2K)
  const formatValue = (v) => {
    if (isNaN(v)) return "-";
    if (Math.abs(v) >= 100000) return (v / 100000).toFixed(1) + " L";
    if (Math.abs(v) >= 1000) return (v / 1000).toFixed(0) + " K";
    return v.toFixed(0);
  };

  // Base chart config
  const baseOptions = {
    layout: { background: { color: "#fff" }, textColor: "#111827" },
    grid: { vertLines: { color: "#f3f4f6" }, horzLines: { color: "#f3f4f6" } },
    crosshair: { 
      mode: LightweightCharts.CrosshairMode.Normal,
      vertLine: { labelVisible: false },
      horzLine: { labelVisible: true }
    },
    localization: { priceFormatter: formatValue },
    rightPriceScale: { visible: true },
    timeScale: {
      borderVisible: false,
      timeVisible: false,
      tickMarkFormatter: (index) => strikes[index] || "",
    }
  };

  // ==============================
  // 🧩 Chart Block Creator
  // ==============================
  function createChartBlock(title, legendItems, ceData, peData, maxCE, maxPE) {
    const block = document.createElement("div");
    block.className = "tv-chart-block";
    container.appendChild(block);

    const chart = LightweightCharts.createChart(block, baseOptions);

    // Series
    const ceSeries = chart.addHistogramSeries({ color: legendItems[2].color });
    const peSeries = chart.addHistogramSeries({ color: legendItems[3].color });
    ceSeries.setData(ceData);
    peSeries.setData(peData);

    // Max Lines
    const maxCELine = chart.addLineSeries({
      color: legendItems[0].color,
      lineStyle: 2,
      lineWidth: 1,
    });
    const maxPELine = chart.addLineSeries({
      color: legendItems[1].color,
      lineStyle: 2,
      lineWidth: 1,
    });
    maxCELine.setData([maxCE]);
    maxPELine.setData([maxPE]);

    chart.timeScale().fitContent();

    // Title
    const titleLabel = document.createElement("div");
    titleLabel.className = "chart-inline-title";
    titleLabel.innerText = title;
    block.appendChild(titleLabel);

    // Legend
    const legend = document.createElement("div");
    legend.className = "chart-inline-legend";
    legendItems.forEach((item) => {
      const el = document.createElement("div");
      el.className = "legend-item active";
      el.dataset.key = item.key;
      el.innerHTML = `<span class="legend-dot" style="border:2px solid ${item.color}"></span>${item.label}`;
      legend.appendChild(el);
    });
    block.appendChild(legend);

    // Legend toggle
    const seriesMap = { maxCE: maxCELine, maxPE: maxPELine, ce: ceSeries, pe: peSeries };
    legend.querySelectorAll(".legend-item").forEach((item) => {
      item.addEventListener("click", () => {
        item.classList.toggle("inactive");
        const key = item.dataset.key;
        seriesMap[key].applyOptions({
          visible: !item.classList.contains("inactive"),
        });
      });
    });

    // Tooltip
    const tooltip = document.createElement("div");
    tooltip.className = "oi-tooltip";
    tooltip.style.display = "none";
    block.appendChild(tooltip);

    chart.subscribeCrosshairMove((param) => {
      if (!param?.time || !param?.point) {
        tooltip.style.display = "none";
        return;
      }

      const idx = param.time;
      const strike = strikes[idx];
      if (strike === undefined) return;

      tooltip.innerHTML = `
        <b>Strike:</b> ${strike}<br>
        <span style="color:${legendItems[2].color}">${legendItems[2].label}:</span> ${formatValue(chartData.ce_oi[idx] || chartData.ce_oi_chg[idx] || 0)}<br>
        <span style="color:${legendItems[3].color}">${legendItems[3].label}:</span> ${formatValue(chartData.pe_oi[idx] || chartData.pe_oi_chg[idx] || 0)}
      `;
      tooltip.style.left = param.point.x + 15 + "px";
      tooltip.style.top = param.point.y + 15 + "px";
      tooltip.style.display = "block";

      // Hover strike label
      if (window.syncLabelEl) {
        window.syncLabelEl.textContent = `Strike: ${strike}`;
      }
    });

    window.addEventListener("resize", () =>
      chart.applyOptions({ width: block.clientWidth })
    );

    return chart;
  }

  // ==============================
  // 📊 Prepare Data
  // ==============================
  const ceOI = strikes.map((s, i) => ({ time: i, value: chartData.ce_oi[i] }));
  const peOI = strikes.map((s, i) => ({ time: i, value: chartData.pe_oi[i] }));
  const ceChg = strikes.map((s, i) => ({ time: i, value: chartData.ce_oi_chg[i] }));
  const peChg = strikes.map((s, i) => ({ time: i, value: chartData.pe_oi_chg[i] }));

  const maxCEVal = Math.max(...chartData.ce_oi);
  const maxPEVal = Math.max(...chartData.pe_oi);
  const maxCE = { time: chartData.ce_oi.indexOf(maxCEVal), value: maxCEVal };
  const maxPE = { time: chartData.pe_oi.indexOf(maxPEVal), value: maxPEVal };

  const maxCEChgVal = Math.max(...chartData.ce_oi_chg);
  const maxPEChgVal = Math.max(...chartData.pe_oi_chg);
  const maxCEChg = { time: chartData.ce_oi_chg.indexOf(maxCEChgVal), value: maxCEChgVal };
  const maxPEChg = { time: chartData.pe_oi_chg.indexOf(maxPEChgVal), value: maxPEChgVal };

  // Floating label
  const label = document.createElement("div");
  label.id = "strike-hover-label";
  label.style.textAlign = "center";
  label.style.fontWeight = "600";
  label.style.marginBottom = "8px";
  label.style.color = "#374151";
  label.textContent = "Strike: —";
  container.prepend(label);
  window.syncLabelEl = label;

  // ==============================
  // 🟢 Create Charts
  // ==============================
  const oiChart = createChartBlock(
    `${ticker}-${expiry} OI`,
    [
      { key: "maxCE", label: "Max CE OI", color: "#22c55e" },
      { key: "maxPE", label: "Max PE OI", color: "#ef4444" },
      { key: "ce", label: "CE OI", color: "#22c55e" },
      { key: "pe", label: "PE OI", color: "#ef4444" },
    ],
    ceOI,
    peOI,
    maxCE,
    maxPE
  );

  const oiChangeChart = createChartBlock(
    `${ticker}-${expiry} OI Change`,
    [
      { key: "maxCE", label: "Max CE OI Chg", color: "#22c55e" },
      { key: "maxPE", label: "Max PE OI Chg", color: "#ef4444" },
      { key: "ce", label: "CE OI Chg", color: "#86efac" },
      { key: "pe", label: "PE OI Chg", color: "#fca5a5" },
    ],
    ceChg,
    peChg,
    maxCEChg,
    maxPEChg
  );

  // 🔗 Sync crosshair
  oiChart.subscribeCrosshairMove((param) => {
    if (!param?.time) return;
    oiChangeChart.moveCrosshair({ x: param.point?.x || 0, y: 0 });
  });

  oiChangeChart.subscribeCrosshairMove((param) => {
    if (!param?.time) return;
    oiChart.moveCrosshair({ x: param.point?.x || 0, y: 0 });
  });
});
