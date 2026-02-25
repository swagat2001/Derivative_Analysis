/* ==========================================================
    STOCK DETAIL OI CHARTS — TradingView Layout
   ========================================================== */

   document.addEventListener("DOMContentLoaded", function () {
    const container = document.getElementById("oi-chart-container");
    if (!container) return;

    let chartData;
    try {
      chartData = JSON.parse(container.dataset.chart || "{}");
    } catch (err) {
      console.error(" Failed to parse chart data:", err);
      return;
    }

    if (!chartData || !chartData.strikes) {
      console.warn(" No chart data found or invalid structure.");
      return;
    }

    const ticker = chartData.meta?.ticker || "Unknown";
    const expiry = chartData.meta?.expiry || "Unknown";
    const strikes = chartData.strikes;
    const underlyingPrice = chartData.underlying_price;
    const futuresPrices = chartData.futures_prices || [];

    // Helper to format large numbers (e.g. 25.3L, 10.2K) - preserves sign
    const formatValue = (v) => {
      if (isNaN(v)) return "-";
      const sign = v < 0 ? "-" : "";
      const absV = Math.abs(v);
      if (absV >= 100000) return sign + (absV / 100000).toFixed(1) + " L";
      if (absV >= 1000) return sign + (absV / 1000).toFixed(0) + " K";
      return v.toFixed(0);
    };

    // Helper to find closest strike index for a given price
    const findClosestStrikeIndex = (price) => {
      if (!price || strikes.length === 0) return null;
      let closestIndex = 0;
      let minDiff = Math.abs(strikes[0] - price);
      for (let i = 1; i < strikes.length; i++) {
        const diff = Math.abs(strikes[i] - price);
        if (diff < minDiff) {
          minDiff = diff;
          closestIndex = i;
        }
      }
      return closestIndex;
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
    //  Chart Block Creator
    // ==============================
    function createChartBlock(title, legendItems, ceData, peData, maxCE, maxPE, isChangeChart = false) {
      const block = document.createElement("div");
      block.className = "tv-chart-block";
      container.appendChild(block);

      const chart = LightweightCharts.createChart(block, baseOptions);

      // Calculate max value for bar height
      const maxValue = Math.max(...ceData.map(d => d.value), ...peData.map(d => d.value));

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

      //  Add Underlying Price Vertical Line (DOM overlay)
      let underlyingBarSeries = null; // kept for seriesMap API compatibility via wrapper
      let underlyingBarIndex = null;
      const spotColor = '#FF9800';
      // container for overlay lines
      const overlayLayer = document.createElement('div');
      overlayLayer.style.position = 'absolute';
      overlayLayer.style.inset = '0 0 0 0';
      overlayLayer.style.pointerEvents = 'none';
      overlayLayer.style.zIndex = '1000';
      // ensure the chart container is relatively positioned
      const chartContainerEl = block.querySelector('div');
      if (chartContainerEl) {
        chartContainerEl.style.position = chartContainerEl.style.position || 'relative';
        chartContainerEl.appendChild(overlayLayer);
      }
      const verticalLines = [];
      function createVerticalLineAt(index, color, className) {
        const lineEl = document.createElement('div');
        lineEl.className = className || 'vline';
        lineEl.style.position = 'absolute';
        lineEl.style.top = '0px';
        lineEl.style.bottom = '0px';
        lineEl.style.width = '3px';
        lineEl.style.background = color;
        lineEl.style.opacity = '0.85';
        lineEl.style.transform = 'translateX(-1.5px)';
        overlayLayer.appendChild(lineEl);
        const updater = () => {
          const x = chart.timeScale().logicalToCoordinate(index);
          if (x === null || x === undefined) {
            lineEl.style.display = 'none';
            return;
          }
          lineEl.style.display = '';
          lineEl.style.left = `${Math.round(x)}px`;
        };
        // initial place
        updater();
        // keep updated on scale/resize
        chart.timeScale().subscribeVisibleTimeRangeChange(updater);
        if (chart.timeScale().subscribeVisibleLogicalRangeChange) {
          chart.timeScale().subscribeVisibleLogicalRangeChange(updater);
        }
        window.addEventListener('resize', updater);
        verticalLines.push({ index, el: lineEl, updater });
        // return a lightweight wrapper mimicking series API for legend toggling
        return {
          applyOptions: (opts) => {
            if (opts && Object.prototype.hasOwnProperty.call(opts, 'visible')) {
              lineEl.style.display = opts.visible ? '' : 'none';
            }
          }
        };
      }
      if (underlyingPrice) {
        const underlyingIndex = findClosestStrikeIndex(underlyingPrice);
        if (underlyingIndex !== null) {
          underlyingBarIndex = underlyingIndex;
          underlyingBarSeries = createVerticalLineAt(underlyingIndex, spotColor, 'vline-spot');
        }
      }

      //  Add Futures Expiry Vertical Lines - 3 lines
      const futuresColors = ['#2196F3', '#9C27B0', '#4CAF50'];
      const futuresBarSeries = [];
      const futuresBarIndices = [];
      futuresPrices.forEach((future, idx) => {
        if (future.price) {
          const futureIndex = findClosestStrikeIndex(future.price);
          if (futureIndex !== null) {
            futuresBarIndices.push(futureIndex);
            const wrapper = createVerticalLineAt(futureIndex, futuresColors[idx], `vline-f${idx + 1}`);
            futuresBarSeries.push(wrapper);
          } else {
            futuresBarIndices.push(null);
            futuresBarSeries.push(null);
          }
        } else {
          futuresBarIndices.push(null);
          futuresBarSeries.push(null);
        }
      });

      chart.timeScale().fitContent();
      // ensure all lines are positioned after fitContent establishes range
      const updateAllLines = () => verticalLines.forEach(v => v.updater());
      updateAllLines();

      // Title
      const titleLabel = document.createElement("div");
      titleLabel.className = "chart-inline-title";
      titleLabel.innerText = title;
      block.appendChild(titleLabel);

      //  Build extended legend items (original + spot + futures)
      const extendedLegendItems = [...legendItems];

      // Add Spot legend item
      if (underlyingPrice && underlyingBarSeries) {
        extendedLegendItems.push({
          key: 'spot',
          label: `Spot: ${underlyingPrice.toFixed(0)}`,
          color: spotColor
        });
      }

      // Add Futures legend items
      futuresPrices.forEach((future, idx) => {
        if (future.price && futuresBarSeries[idx] !== null && futuresBarSeries[idx] !== undefined) {
          const expiryDate = future.expiry ? new Date(future.expiry).toLocaleDateString('en-US', { month: 'short', day: 'numeric' }) : '';
          extendedLegendItems.push({
            key: `f${idx + 1}`,
            label: `F${idx + 1}: ${future.price.toFixed(0)}${expiryDate ? ` (${expiryDate})` : ''}`,
            color: futuresColors[idx]
          });
        }
      });

      // Legend
      const legend = document.createElement("div");
      legend.className = "chart-inline-legend";
      extendedLegendItems.forEach((item) => {
        const el = document.createElement("div");
        el.className = "legend-item active";
        el.dataset.key = item.key;
        el.innerHTML = `<span class="legend-dot" style="border:2px solid ${item.color}"></span>${item.label}`;
        legend.appendChild(el);
      });
      block.appendChild(legend);

      //  Build series map including spot and futures bars
      const seriesMap = {
        maxCE: maxCELine,
        maxPE: maxPELine,
        ce: ceSeries,
        pe: peSeries
      };

      if (underlyingBarSeries) {
        seriesMap['spot'] = underlyingBarSeries;
      }

      futuresBarSeries.forEach((bar, idx) => {
        if (bar) {
          seriesMap[`f${idx + 1}`] = bar;
        }
      });

      // Legend toggle
      legend.querySelectorAll(".legend-item").forEach((item) => {
        item.addEventListener("click", () => {
          item.classList.toggle("inactive");
          const key = item.dataset.key;
          if (seriesMap[key]) {
            seriesMap[key].applyOptions({
              visible: !item.classList.contains("inactive"),
            });
          }
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

        //  Get raw values directly from the chart data
        const ceValue = isChangeChart ? chartData.ce_oi_chg[idx] : chartData.ce_oi[idx];
        const peValue = isChangeChart ? chartData.pe_oi_chg[idx] : chartData.pe_oi[idx];

        // Build tooltip content
        let tooltipContent = `
          <b>Strike:</b> ${strike}<br>
          <span style="color:${legendItems[2].color}">${legendItems[2].label}:</span> ${formatValue(ceValue || 0)}<br>
          <span style="color:${legendItems[3].color}">${legendItems[3].label}:</span> ${formatValue(peValue || 0)}
        `;

        //  Check if hovering over Spot bar
        if (underlyingBarIndex !== null && idx === underlyingBarIndex) {
          tooltipContent += `<br><span style="color:${spotColor}"><b>Spot Price:</b> ${underlyingPrice.toFixed(2)}</span>`;
        }

        //  Check if hovering over any Future bar
        futuresBarIndices.forEach((futureIdx, futureArrayIdx) => {
          if (futureIdx !== null && idx === futureIdx && futuresPrices[futureArrayIdx]) {
            const future = futuresPrices[futureArrayIdx];
            const expiryDate = future.expiry ? new Date(future.expiry).toLocaleDateString('en-US', { month: 'short', day: 'numeric', year: 'numeric' }) : 'N/A';
            tooltipContent += `<br><span style="color:${futuresColors[futureArrayIdx]}"><b>F${futureArrayIdx + 1}:</b> ${future.price.toFixed(2)} (Exp: ${expiryDate})</span>`;
          }
        });

        tooltip.innerHTML = tooltipContent;
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
    //  Prepare Data
    // ==============================
    const ceOI = strikes.map((s, i) => ({ time: i, value: chartData.ce_oi[i] }));
    const peOI = strikes.map((s, i) => ({ time: i, value: chartData.pe_oi[i] }));
    //  Keep negative values for OI Change (don't use absolute)
    const ceChg = strikes.map((s, i) => ({ time: i, value: chartData.ce_oi_chg[i] || 0 }));
    const peChg = strikes.map((s, i) => ({ time: i, value: chartData.pe_oi_chg[i] || 0 }));

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
    //  Create Charts
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
      maxPE,
      false
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
      maxPEChg,
      true
    );

    //  Sync crosshair
    oiChart.subscribeCrosshairMove((param) => {
      if (!param?.time) return;
      oiChangeChart.moveCrosshair({ x: param.point?.x || 0, y: 0 });
    });

    oiChangeChart.subscribeCrosshairMove((param) => {
      if (!param?.time) return;
      oiChart.moveCrosshair({ x: param.point?.x || 0, y: 0 });
    });
  });
