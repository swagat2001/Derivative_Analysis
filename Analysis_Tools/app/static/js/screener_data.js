/* ===== SVG ICON DEFINITIONS ===== */
const ICONS = {
  chart: '<svg width="20" height="20" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><path d="M3 3v18h18"/><path d="m19 9-5 5-4-4-3 3"/></svg>',
  target: '<svg width="20" height="20" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><circle cx="12" cy="12" r="10"/><circle cx="12" cy="12" r="6"/><circle cx="12" cy="12" r="2"/></svg>',
  package: '<svg width="20" height="20" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><path d="M21 16V8a2 2 0 0 0-1-1.73l-7-4a2 2 0 0 0-2 0l-7 4A2 2 0 0 0 3 8v8a2 2 0 0 0 1 1.73l7 4a2 2 0 0 0 2 0l7-4A2 2 0 0 0 21 16z"/><polyline points="3.27 6.96 12 12.01 20.73 6.96"/><line x1="12" y1="22.08" x2="12" y2="12"/></svg>',
  trendUp: '<svg width="20" height="20" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><polyline points="22 7 13.5 15.5 8.5 10.5 2 17"/><polyline points="16 7 22 7 22 13"/></svg>',
  sparkles: '<svg width="20" height="20" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><path d="M12 3l1.5 4.5L18 9l-4.5 1.5L12 15l-1.5-4.5L6 9l4.5-1.5L12 3z"/><path d="M5 19l.5 1.5L7 21l-1.5.5L5 23l-.5-1.5L3 21l1.5-.5L5 19z"/><path d="M19 5l.5 1.5L21 7l-1.5.5L19 9l-.5-1.5L17 7l1.5-.5L19 5z"/></svg>',
  trendDown: '<svg width="20" height="20" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><polyline points="22 17 13.5 8.5 8.5 13.5 2 7"/><polyline points="16 17 22 17 22 11"/></svg>',
  flame: '<svg width="20" height="20" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><path d="M8.5 14.5A2.5 2.5 0 0 0 11 12c0-1.38-.5-2-1-3-1.072-2.143-.224-4.054 2-6 .5 2.5 2 4.9 4 6.5 2 1.6 3 3.5 3 5.5a7 7 0 1 1-14 0c0-1.153.433-2.294 1-3a2.5 2.5 0 0 0 2.5 2.5z"/></svg>',
  snowflake: '<svg width="20" height="20" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><line x1="12" y1="2" x2="12" y2="22"/><path d="m4.93 4.93 4.24 4.24"/><path d="m14.83 14.83 4.24 4.24"/><path d="m4.93 19.07 4.24-4.24"/><path d="m14.83 9.17 4.24-4.24"/><line x1="2" y1="12" x2="22" y2="12"/></svg>',
  strength: '<svg width="20" height="20" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><path d="M12 20V10"/><path d="M18 20V4"/><path d="M6 20v-4"/></svg>',
  trophy: '<svg width="20" height="20" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><path d="M6 9H4.5a2.5 2.5 0 0 1 0-5H6"/><path d="M18 9h1.5a2.5 2.5 0 0 0 0-5H18"/><path d="M4 22h16"/><path d="M10 14.66V17c0 .55-.47.98-.97 1.21C7.85 18.75 7 20.24 7 22"/><path d="M14 14.66V17c0 .55.47.98.97 1.21C16.15 18.75 17 20.24 17 22"/><path d="M18 2H6v7a6 6 0 0 0 12 0V2Z"/></svg>',
  bank: '<svg width="20" height="20" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><line x1="3" y1="22" x2="21" y2="22"/><line x1="6" y1="18" x2="6" y2="11"/><line x1="10" y1="18" x2="10" y2="11"/><line x1="14" y1="18" x2="14" y2="11"/><line x1="18" y1="18" x2="18" y2="11"/><polygon points="12 2 20 7 4 7"/></svg>',
  download: '<svg width="20" height="20" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><path d="M21 15v4a2 2 0 0 1-2 2H5a2 2 0 0 1-2-2v-4"/><polyline points="7 10 12 15 17 10"/><line x1="12" y1="15" x2="12" y2="3"/></svg>',
  zap: '<svg width="20" height="20" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><polygon points="13 2 3 14 12 14 11 22 21 10 12 10 13 2"/></svg>',
  arrowUp: '<svg width="20" height="20" viewBox="0 0 24 24" fill="none" stroke="#16a34a" stroke-width="2"><circle cx="12" cy="12" r="10"/><polyline points="16 12 12 8 8 12"/><line x1="12" y1="16" x2="12" y2="8"/></svg>',
  arrowDown: '<svg width="20" height="20" viewBox="0 0 24 24" fill="none" stroke="#dc2626" stroke-width="2"><circle cx="12" cy="12" r="10"/><polyline points="8 12 12 16 16 12"/><line x1="12" y1="8" x2="12" y2="16"/></svg>',
  volume: '<svg width="20" height="20" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><polygon points="11 5 6 9 2 9 2 15 6 15 11 19 11 5"/><path d="M15.54 8.46a5 5 0 0 1 0 7.07"/><path d="M19.07 4.93a10 10 0 0 1 0 14.14"/></svg>',
  rocket: '<svg width="20" height="20" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><path d="M4.5 16.5c-1.5 1.26-2 5-2 5s3.74-.5 5-2c.71-.84.7-2.13-.09-2.91a2.18 2.18 0 0 0-2.91-.09z"/><path d="m12 15-3-3a22 22 0 0 1 2-3.95A12.88 12.88 0 0 1 22 2c0 2.72-.78 7.5-6 11a22.35 22.35 0 0 1-4 2z"/><path d="M9 12H4s.55-3.03 2-4c1.62-1.08 5 0 5 0"/><path d="M12 15v5s3.03-.55 4-2c1.08-1.62 0-5 0-5"/></svg>'
};

/* ===== SCREENER DATA WITH ROUTES ===== */
const SCREENER_DATA = {
  "Derivative Scanners": [
    {
      title: "F&O Signal Scanner",
      description: "Comprehensive F&O scanner with RSI, Pivot Levels, Volume Profile, OI Spike & Divergence signals",
      icon: ICONS.target,
      url: "/scanner/signal-scanner/",
      badge: "New",
      inline: false
    },
    {
      title: "Top Gainers & Losers",
      description: "Top 10 gainers/losers across 40 categories - OI, IV, Moneyness for Calls, Puts & Futures",
      icon: ICONS.chart,
      url: "/scanner/top-gainers-losers/",
      badge: "Popular",
      inline: false
    },
    {
      title: "Signal Analysis",
      description: "Advanced bullish/bearish signal classification with multi-factor analysis",
      icon: ICONS.sparkles,
      url: "/scanner/signal-analysis/",
      badge: null,
      inline: false
    },
    {
      title: "Futures OI Analysis",
      description: "Expiry-wise Open Interest analysis - Current, Next & Far Month",
      icon: ICONS.package,
      url: "/scanner/futures-oi/",
      badge: null,
      inline: false
    },
    {
      title: "F&O Greeks analysis",
      description: "Detailed analysis of F&O Greeks, Delta, Vega and Moneyness",
      icon: ICONS.chart,
      url: "/dashboard/",
      badge: "New",
      inline: false
    }
  ],
  "Technical Scanners": [
    {
      title: "Technical Scanner",
      description: "RSI, MACD, SMA, ADX based screening with interactive heatmaps",
      icon: ICONS.trendUp,
      url: "/scanner/technical-indicators/",
      badge: "Premium",
      inline: false
    },
    {
      title: "Golden Crossover",
      description: "Stocks where 50-day SMA crosses above 200-day SMA — bullish trend reversal",
      icon: ICONS.sparkles,
      url: "/scanner/technical-indicators/golden-crossover",
      badge: null,
      inline: false
    },
    {
      title: "Death Crossover",
      description: "Stocks where 50-day SMA crosses below 200-day SMA — bearish trend reversal",
      icon: ICONS.trendDown,
      url: "/scanner/technical-indicators/death-crossover",
      badge: null,
      inline: false
    },
    {
      title: "Overbought RSI Stocks",
      description: "Stocks with RSI > 75 suggesting possible upcoming price correction",
      icon: ICONS.flame,
      url: "/scanner/technical-indicators/rsi-overbought",
      badge: "New",
      inline: false
    },
    {
      title: "Oversold RSI Stocks",
      description: "Stocks with RSI < 25 indicating potential price recovery",
      icon: ICONS.snowflake,
      url: "/scanner/technical-indicators/rsi-oversold",
      badge: "New",
      inline: false
    },
    {
      title: "R1 Resistance Breakouts",
      description: "Stocks climbing past R1 resistance, signaling sustained upward trends",
      icon: ICONS.arrowUp,
      url: "/scanner/technical-indicators/r1-breakout",
      badge: "New",
      inline: false
    },
    {
      title: "R2 Resistance Breakouts",
      description: "Stocks breaking above R2 resistance with strong bullish momentum",
      icon: ICONS.arrowUp,
      url: "/scanner/technical-indicators/r2-breakout",
      badge: null,
      inline: false
    },
    {
      title: "R3 Resistance Breakouts",
      description: "Stocks breaking above R3 resistance — extremely strong bullish signal",
      icon: ICONS.rocket,
      url: "/scanner/technical-indicators/r3-breakout",
      badge: null,
      inline: false
    },
    {
      title: "S1 Support Breakouts",
      description: "Stocks falling through S1 support, possibly forecasting extended declines",
      icon: ICONS.arrowDown,
      url: "/scanner/technical-indicators/s1-breakout",
      badge: "New",
      inline: false
    },
    {
      title: "S2 Support Breakouts",
      description: "Stocks falling through S2 support, indicating significant bearish pressure",
      icon: ICONS.arrowDown,
      url: "/scanner/technical-indicators/s2-breakout",
      badge: null,
      inline: false
    },
    {
      title: "S3 Support Breakouts",
      description: "Stocks falling through S3 support — extreme bearish signal",
      icon: ICONS.arrowDown,
      url: "/scanner/technical-indicators/s3-breakout",
      badge: null,
      inline: false
    },
    {
      title: "Strong Trend (ADX > 25)",
      description: "Stocks with ADX above 25, indicating a strong trend is in progress",
      icon: ICONS.strength,
      url: "/scanner/technical-indicators/strong-adx-trend",
      badge: null,
      inline: false
    },
    {
      title: "Above 50 & 200 SMA",
      description: "Strong bullish setup where price is trading above both major moving averages",
      icon: ICONS.trophy,
      url: "/scanner/technical-indicators/above-50-200-sma",
      badge: "Bullish",
      inline: false
    },
    {
      title: "Below 50 & 200 SMA",
      description: "Weak bearish setup where price is trading below both major moving averages",
      icon: ICONS.snowflake,
      url: "/scanner/technical-indicators/below-50-200-sma",
      badge: "Bearish",
      inline: false
    },
    {
      title: "MACD Bullish Cross",
      description: "Stocks where MACD line has crossed above the signal line",
      icon: ICONS.rocket,
      url: "/scanner/technical-indicators/macd-bullish-cross",
      badge: "Bullish",
      inline: false
    },
    {
      title: "MACD Bearish Cross",
      description: "Stocks where MACD line has crossed below the signal line",
      icon: ICONS.arrowDown,
      url: "/scanner/technical-indicators/macd-bearish-cross",
      badge: "Bearish",
      inline: false
    },
    {
      title: "High Volume Stocks",
      description: "Stocks with the highest trading volume in the current session",
      icon: ICONS.volume,
      url: "/scanner/technical-indicators/high-volume",
      badge: "Popular",
      inline: false
    },
    {
      title: "Momentum Stocks",
      description: "Securities surging in price with strong market enthusiasm and potential for further gains",
      icon: ICONS.rocket,
      url: "/scanner/technical-indicators/momentum-stocks",
      badge: "New",
      inline: false
    },
    {
      title: "Squeezing Range",
      description: "Stocks with tightening Bollinger Bands, indicating a potential breakout or breakdown",
      icon: ICONS.target,
      url: "/scanner/technical-indicators/squeezing-range",
      badge: "New",
      inline: false
    }
  ],
  "Fundamental Scanners": [
    {
      title: "Best Annual Results",
      description: "Companies with consistent sales & profit growth",
      icon: ICONS.chart,
      dataKey: "scanx-growth",
      apiUrl: "/scanner/fundamental/api/scan/best_results",
      badge: "Hot",
      inline: true
    },
    {
      title: "CapEx Boost",
      description: "Businesses ramping up capital expenditures",
      icon: ICONS.rocket,
      dataKey: "scanx-capex",
      apiUrl: "/scanner/fundamental/api/scan/capex_boost",
      badge: "New",
      inline: true
    },
    {
      title: "Midcap Stocks",
      description: "Mid-caps with robust fundamentals and growth",
      icon: ICONS.strength,
      dataKey: "scanx-midcap",
      apiUrl: "/scanner/fundamental/api/scan/mighty_midcap",
      badge: null,
      inline: true
    },
    {
      title: "Largecap Stocks",
      description: "Stable market leaders with strong positions",
      icon: ICONS.trophy,
      dataKey: "scanx-largecap",
      apiUrl: "/scanner/fundamental/api/scan/titan_largecap",
      badge: null,
      inline: true
    },
    {
      title: "Smallcap Stocks",
      description: "High-performing small-caps with upside",
      icon: ICONS.sparkles,
      dataKey: "scanx-smallcap",
      apiUrl: "/scanner/fundamental/api/scan/stellar_smallcap",
      badge: null,
      inline: true
    },
    {
      title: "Negative Working Capital",
      description: "Efficient liquidity management",
      icon: ICONS.zap,
      dataKey: "scanx-nwc",
      apiUrl: "/scanner/fundamental/api/scan/negative_working_capital",
      badge: null,
      inline: true
    },
    {
      title: "Multibagger",
      description: "High growth metrics with upside potential",
      icon: ICONS.target,
      dataKey: "scanx-multi",
      apiUrl: "/scanner/fundamental/api/scan/potential_multibagger",
      badge: "Pop",
      inline: true
    }
  ],
  "Intraday Scanners": [
    {
      title: "Price Gainers",
      description: "Top price gainers in {date} trading session",
      icon: ICONS.arrowUp,
      url: "/scanner/technical-indicators/price-gainers",
      badge: null,
      inline: false
    },
    {
      title: "Price Losers",
      description: "Top price losers in {date} trading session",
      icon: ICONS.arrowDown,
      url: "/scanner/technical-indicators/price-losers",
      badge: null,
      inline: false
    },
    {
      title: "High Volume Alert",
      description: "Stocks trading above average volume",
      icon: ICONS.volume,
      url: "/scanner/technical-indicators/potential-high-volume",
      badge: null,
      inline: false
    },
    {
      title: "Breakout Scanner",
      description: "Stocks breaking key resistance levels",
      icon: ICONS.rocket,
      url: "/scanner/technical-indicators/?filter=breakout",
      badge: "Beta",
      inline: false
    },
    {
      title: "Nifty 50 Analysis",
      description: "Comprehensive derivative analysis for Nifty 50 stocks",
      icon: ICONS.trophy,
      dataKey: "nifty50",
      apiUrl: "/scanner/api/nifty50",
      badge: null,
      inline: true
    },
    {
      title: "Bank Nifty Analysis",
      description: "Derivative analysis for Bank Nifty constituents",
      icon: ICONS.bank,
      dataKey: "banknifty",
      apiUrl: "/scanner/api/banknifty",
      badge: null,
      inline: true
    },
    {
      title: "High OI Buildup",
      description: "Stocks showing significant Open Interest accumulation",
      icon: ICONS.download,
      dataKey: "high-oi",
      apiUrl: "/scanner/api/high-oi",
      badge: null,
      inline: true
    },
    {
      title: "IV Spike Alert",
      description: "Stocks with unusual Implied Volatility movement",
      icon: ICONS.zap,
      dataKey: "iv-spike",
      apiUrl: "/scanner/api/iv-spike",
      badge: null,
      inline: true
    }
  ],
  "Price & Volume Scanners": [
    {
      title: "1 Week High Breakouts",
      description: "Securities surpassing highest price in the last week reflecting immediate bullish sentiment",
      icon: ICONS.rocket,
      url: "/scanner/technical-indicators/week1-high-breakout",
      badge: "New",
      inline: false
    },
    {
      title: "1 Week Low Breakouts",
      description: "Indicates immediate bearish sentiment with decline past their lowest price in the previous week",
      icon: ICONS.arrowDown,
      url: "/scanner/technical-indicators/week1-low-breakout",
      badge: "New",
      inline: false
    },
    {
      title: "4 Week High Breakouts",
      description: "Securities exceeding their highest price in the last month indicating short-term bullish momentum",
      icon: ICONS.rocket,
      url: "/scanner/technical-indicators/week4-high-breakout",
      badge: null,
      inline: false
    },
    {
      title: "4 Week Low Breakouts",
      description: "Pointing to short-term bearish momentum with stocks falling below their lowest price in the past month",
      icon: ICONS.arrowDown,
      url: "/scanner/technical-indicators/week4-low-breakout",
      badge: null,
      inline: false
    },
    {
      title: "52 Week High Breakouts",
      description: "Breaking past their highest price in the last year signaling strong bullish trends",
      icon: ICONS.trophy,
      url: "/scanner/technical-indicators/week52-high-breakout",
      badge: "Hot",
      inline: false
    },
    {
      title: "52 Week Low Breakouts",
      description: "Strong bearish trends suggested by dropping below their lowest price in the past year",
      icon: ICONS.snowflake,
      url: "/scanner/technical-indicators/week52-low-breakout",
      badge: null,
      inline: false
    },
    {
      title: "Potential High Volume",
      description: "Showing early signs of a spike in trading volume hinting at upcoming activity",
      icon: ICONS.volume,
      url: "/scanner/technical-indicators/potential-high-volume",
      badge: null,
      inline: false
    },
    {
      title: "Unusually High Volume",
      description: "Points to increased interest or activity with volume much higher than average",
      icon: ICONS.flame,
      url: "/scanner/technical-indicators/unusually-high-volume",
      badge: "Hot",
      inline: false
    }
  ]
};

/* ===== STOCK DATA STORAGE (Fetched via API) ===== */
const STOCK_DATA = {};

console.log(' screener_data.js loaded -', Object.keys(SCREENER_DATA).length, 'categories');
