from flask import Blueprint, jsonify, render_template

from ....services.fundamental_service import fundamental_service

# Define Blueprint
fundamental_screener_bp = Blueprint("fundamental_screener", __name__, url_prefix="/screener/fundamental")


@fundamental_screener_bp.route("/api/scan/<category>")
def get_scan_results(category):
    try:
        results = []
        name = ""
        description = ""
        tag = ""

        if category == "capex_boost":
            # Criteria: High Increase in Capex (> 50%)
            # Using 'capex_growth_pct' calculated in service
            results = fundamental_service.filter_stocks(lambda x: x["capex_growth_pct"] > 50)
            results.sort(key=lambda x: x["capex_growth_pct"], reverse=True)
            name = "CapEx Boost"
            description = "Businesses with >50% increase in Capital Expenditure/Investing."
            tag = "Growth"

        elif category == "titan_largecap":
            # Criteria: Sales > 20,000 Cr (Proxy for Large Cap)
            results = fundamental_service.filter_stocks(lambda x: x["sales"] > 20000)
            results.sort(key=lambda x: x["sales"], reverse=True)
            name = "Titan Largecap Stocks"
            description = "Established market leaders with Sales > 20,000 Cr."
            tag = "Stability"

        elif category == "mighty_midcap":
            # Criteria: Sales between 5,000 and 20,000 Cr
            results = fundamental_service.filter_stocks(lambda x: 5000 < x["sales"] < 20000)
            results.sort(key=lambda x: x["profit_growth_3yr"], reverse=True)
            name = "Mighty Midcap Stocks"
            description = "Mid-sized companies (Sales 5k-20k Cr) with growth potential."
            tag = "Growth"

        elif category == "stellar_smallcap":
            # Criteria: Sales < 5,000 Cr and Positive Proft
            results = fundamental_service.filter_stocks(lambda x: 100 < x["sales"] < 5000 and x["net_profit"] > 0)
            results.sort(key=lambda x: x["profit_growth_3yr"], reverse=True)
            name = "Stellar Smallcap Stocks"
            description = "Small companies (Sales < 5k Cr) with positive profits."
            tag = "High Risk/Reward"

        elif category == "negative_working_capital":
            # Criteria: Working Capital Days < 0
            results = fundamental_service.filter_stocks(lambda x: x["working_capital_days"] < 0)
            results.sort(key=lambda x: x["working_capital_days"])  # Most negative first
            name = "Negative Working Capital"
            description = "Efficient companies operating with negative working capital."
            tag = "Efficiency"

        elif category == "potential_multibagger":
            # Criteria: High Sales Growth (>15%) + High Profit Growth (>15%) + Mid/Small Cap (<20k Sales)
            results = fundamental_service.filter_stocks(
                lambda x: x["sales_growth_3yr"] > 15 and x["profit_growth_3yr"] > 15 and x["sales"] < 20000
            )
            results.sort(key=lambda x: x["profit_growth_3yr"], reverse=True)
            name = "Potential Multibagger"
            description = "High growth companies (Sales & Profit > 15% CAGR) with room to run."
            tag = "Multibagger"

        elif category == "best_results":
            # Criteria: Consistent high profit growth (>20% 3yr CAGR)
            results = fundamental_service.filter_stocks(lambda x: x["profit_growth_3yr"] > 20)
            results.sort(key=lambda x: x["profit_growth_3yr"], reverse=True)
            name = "Best Growth Results"
            description = "Companies with >20% Profit Growth (3Yr CAGR)."
            tag = "Earnings"

        else:
            return jsonify({"error": "Invalid category"}), 400

        formatted_stocks = []
        for stock in results[:50]:
            formatted_stocks.append(
                {
                    "ticker": stock["ticker"],
                    "price": stock.get("price", 0),
                    "change": stock.get("change", 0),  # Added in service if possible, or 0
                    "change_pct": stock.get("change_pct", 0),
                    "volume": stock.get("volume", 0),
                    "market_cap": stock.get("market_cap", 0),
                    "oi": stock.get("oi", 0),
                    "iv": stock.get("iv", 0),
                    "pe": stock.get("pe", 0),
                    "signal": stock.get("signal", "NEUTRAL"),
                    "custom_metric_label": "Key Metric",
                    "custom_metric_value": (
                        f"Capex Growth: {stock['capex_growth_pct']:.1f}%"
                        if category == "capex_boost"
                        else f"Sales: {stock['sales']:.0f}Cr"
                        if category in ["titan_largecap", "mighty_midcap", "stellar_smallcap"]
                        else f"Days: {stock['working_capital_days']:.0f}"
                        if category == "negative_working_capital"
                        else f"Growth: {stock['profit_growth_3yr']:.1f}%"
                        if category in ["potential_multibagger", "best_results"]
                        else f"ROCE: {stock['roce']*100:.1f}%"
                    ),
                }
            )

        return jsonify({"title": name, "description": description, "tag": tag, "stocks": formatted_stocks})

    except Exception as e:
        print(f"Error in fundamental scan: {e}")
        return jsonify({"error": str(e)}), 500
