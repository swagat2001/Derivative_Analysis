"""
NEWS CONTROLLER
Fetches and displays comprehensive stock market news using Google News RSS feed.

IMPORTANT: All news articles are sourced from Google News RSS and clearly
attributed to their original publishers. This controller does not store
or republish the content, only links to the original sources.
"""

import re
import xml.etree.ElementTree as ET
from datetime import datetime
from html import unescape
from urllib.parse import quote_plus, urlparse
try:
    from transformers import pipeline
    TRANSFORMERS_AVAILABLE = True
except ImportError:
    TRANSFORMERS_AVAILABLE = False

import requests
from flask import Blueprint, jsonify, render_template, request

from ..controllers.dashboard_controller import get_live_indices
from ..models.stock_model import get_filtered_tickers

news_bp = Blueprint("news", __name__, url_prefix="/news")

# Google News RSS base URL
GOOGLE_NEWS_RSS_BASE = "https://news.google.com/rss/search"

# Comprehensive news categories covering all aspects of stock market
NEWS_CATEGORIES = {
    "top": {
        "name": "Top Stories",
        "query": "stock market India today NSE BSE breaking news",
        "description": "Breaking news and top stories",
    },
    "market": {
        "name": "Market Updates",
        "query": "share market live updates Sensex Nifty today",
        "description": "Live market movements and analysis",
    },
    "corporate": {
        "name": "Corporate News",
        "query": "corporate news India company announcement board meeting",
        "description": "Company announcements and corporate actions",
    },
    "earnings": {
        "name": "Results & Earnings",
        "query": "quarterly results earnings Q3 Q4 profit loss India company",
        "description": "Quarterly results and earnings reports",
    },
    "ipo": {
        "name": "IPO & Listings",
        "query": "IPO listing GMP allotment SME mainboard India",
        "description": "IPO updates, GMP, and new listings",
    },
    "fii": {
        "name": "FII/DII Activity",
        "query": "FII DII buying selling foreign institutional investor India",
        "description": "Foreign and domestic institutional activity",
    },
    "policy": {
        "name": "Policy & RBI",
        "query": "RBI policy interest rate SEBI regulation India market",
        "description": "Regulatory and policy updates",
    },
    "economy": {
        "name": "Economy",
        "query": "India economy GDP inflation fiscal deficit budget",
        "description": "Economic indicators and macro news",
    },
    "banking": {
        "name": "Banking & Finance",
        "query": "banking sector NBFC loan NPA India financial services",
        "description": "Banking and financial sector news",
    },
    "it": {
        "name": "IT & Tech",
        "query": "IT sector TCS Infosys Wipro HCL tech stocks India",
        "description": "Technology and IT sector updates",
    },
    "pharma": {
        "name": "Pharma & Healthcare",
        "query": "pharma sector healthcare Sun Pharma Cipla Dr Reddy India",
        "description": "Pharmaceutical and healthcare news",
    },
    "auto": {
        "name": "Auto & EV",
        "query": "auto sector Tata Motors Maruti Mahindra EV electric vehicle India",
        "description": "Automobile and EV sector news",
    },
    "energy": {
        "name": "Energy & Oil",
        "query": "oil gas Reliance ONGC BPCL crude price energy India",
        "description": "Energy, oil & gas sector updates",
    },
    "metals": {
        "name": "Metals & Mining",
        "query": "steel metals Tata Steel JSW Hindalco mining India",
        "description": "Metals and mining sector news",
    },
    "realty": {
        "name": "Real Estate",
        "query": "real estate realty DLF Godrej Properties housing India",
        "description": "Real estate and construction news",
    },
    "fmcg": {
        "name": "FMCG & Consumer",
        "query": "FMCG consumer goods HUL ITC Nestle Britannia India",
        "description": "FMCG and consumer goods sector",
    },
    "global": {
        "name": "Global Markets",
        "query": "global markets US Fed Dow Jones NASDAQ Wall Street Asia",
        "description": "International market news",
    },
    "commodities": {
        "name": "Commodities",
        "query": "gold silver crude oil MCX commodity prices India",
        "description": "Commodity market updates",
    },
    "forex": {
        "name": "Forex & Currency",
        "query": "rupee dollar forex currency exchange rate India",
        "description": "Currency and forex news",
    },
    "crypto": {
        "name": "Crypto",
        "query": "cryptocurrency Bitcoin Ethereum crypto India regulation",
        "description": "Cryptocurrency news and updates",
    },
    "mf": {
        "name": "Mutual Funds",
        "query": "mutual fund SIP NAV NFO equity fund India",
        "description": "Mutual fund and SIP updates",
    },
}

# Default category
DEFAULT_CATEGORY = "top"


def fetch_google_news_rss(query: str, max_results: int = 30) -> list:
    """
    Fetch news from Google News RSS feed.
    """
    try:
        encoded_query = quote_plus(query)
        rss_url = f"{GOOGLE_NEWS_RSS_BASE}?q={encoded_query}&hl=en-IN&gl=IN&ceid=IN:en"

        headers = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"}
        response = requests.get(rss_url, headers=headers, timeout=10)
        response.raise_for_status()

        root = ET.fromstring(response.content)
        items = root.findall(".//item")

        news_list = []
        for item in items[:max_results]:
            try:
                title_elem = item.find("title")
                link_elem = item.find("link")
                pub_date_elem = item.find("pubDate")
                description_elem = item.find("description")
                source_elem = item.find("source")

                title = unescape(title_elem.text) if title_elem is not None and title_elem.text else "No Title"
                link = link_elem.text if link_elem is not None and link_elem.text else "#"
                pub_date_str = pub_date_elem.text if pub_date_elem is not None else ""
                description = (
                    unescape(description_elem.text) if description_elem is not None and description_elem.text else ""
                )

                # Extract source name
                source_name = "Unknown Source"
                if source_elem is not None and source_elem.text:
                    source_name = source_elem.text
                elif " - " in title:
                    parts = title.rsplit(" - ", 1)
                    if len(parts) == 2:
                        title = parts[0].strip()
                        source_name = parts[1].strip()

                # Parse published date
                published_at = None
                relative_time = "Recently"
                if pub_date_str:
                    try:
                        published_at = datetime.strptime(pub_date_str, "%a, %d %b %Y %H:%M:%S %Z")
                        relative_time = get_relative_time(published_at)
                    except ValueError:
                        try:
                            published_at = datetime.strptime(pub_date_str[:25], "%a, %d %b %Y %H:%M:%S")
                            relative_time = get_relative_time(published_at)
                        except ValueError:
                            pass

                # Clean description
                clean_description = re.sub(r"<[^>]+>", "", description)
                clean_description = unescape(clean_description)
                if len(clean_description) > 200:
                    clean_description = clean_description[:200] + "..."

                news_list.append(
                    {
                        "title": title,
                        "link": link,
                        "source": source_name,
                        "published_at": published_at.isoformat() if published_at else None,
                        "relative_time": relative_time,
                        "description": clean_description,
                    }
                )

            except Exception as e:
                continue

        return news_list

    except Exception as e:
        print(f"[ERROR] fetch_google_news_rss: {e}")
        return []


def fetch_combined_news(queries: list, max_per_query: int = 12, total_max: int = 40) -> list:
    """Fetch from multiple queries and combine/deduplicate."""
    all_news = []
    seen_titles = set()

    for query in queries:
        news_items = fetch_google_news_rss(query, max_results=max_per_query)
        for item in news_items:
            title_lower = item["title"].lower()[:50]
            if title_lower not in seen_titles:
                seen_titles.add(title_lower)
                all_news.append(item)

    all_news.sort(key=lambda x: x.get("published_at") or "", reverse=True)
    return all_news[:total_max]


def get_relative_time(dt: datetime) -> str:
    """Convert datetime to relative time string."""
    try:
        now = datetime.utcnow()
        diff = now - dt
        seconds = diff.total_seconds()

        if seconds < 60:
            return "Just now"
        elif seconds < 3600:
            minutes = int(seconds / 60)
            return f"{minutes} min{'s' if minutes > 1 else ''} ago"
        elif seconds < 86400:
            hours = int(seconds / 3600)
            return f"{hours} hour{'s' if hours > 1 else ''} ago"
        elif seconds < 604800:
            days = int(seconds / 86400)
            return f"{days} day{'s' if days > 1 else ''} ago"
        else:
            return dt.strftime("%d %b %Y")
    except Exception:
        return "Recently"

# Load AI once at startup. This will use about 1.2GB of your 24GB RAM.
if TRANSFORMERS_AVAILABLE:
    print("Initializing FinBERT Sentiment AI...")
    try:
        sentiment_pipe = pipeline("sentiment-analysis", model="ProsusAI/finbert", device=-1)
    except Exception as e:
        print(f"[WARN] Failed to load sentiment pipeline: {e}")
        sentiment_pipe = None
else:
    print("[WARN] Transformers library not found. Sentiment analysis disabled.")
    sentiment_pipe = None

def apply_sentiment_to_news(news_list):
    """Helper function to process headlines in batch for speed."""
    if not news_list:
        return news_list

    # Check if pipeline is available
    if not sentiment_pipe:
        return news_list

    # Extract just titles
    titles = [item['title'] for item in news_list]

    # Run AI batch analysis
    try:
        results = sentiment_pipe(titles, padding=True, truncation=True)
    except Exception as e:
        print(f"[ERROR] Sentiment analysis failed: {e}")
        return news_list

    # Map labels and merge back
    mapping = {"positive": "Bullish", "negative": "Bearish", "neutral": "Neutral"}

    for i in range(len(news_list)):
        label = results[i]['label']
        news_list[i]['sentiment'] = mapping.get(label, "Neutral")
        news_list[i]['sentiment_class'] = news_list[i]['sentiment'].lower() # for CSS
        news_list[i]['confidence'] = f"{round(results[i]['score'] * 100, 1)}%"

    return news_list


# ========================================================================
# ROUTES
# ========================================================================


@news_bp.route("/")
def news_page():
    """Main news page with category tabs"""
    category = request.args.get("category", DEFAULT_CATEGORY)

    if category not in NEWS_CATEGORIES:
        category = DEFAULT_CATEGORY

    category_info = NEWS_CATEGORIES[category]

    # For "top" category, fetch from multiple sources
    if category == "top":
        queries = [
            "stock market India breaking news today",
            "Sensex Nifty market update",
            "share market news NSE BSE",
            "corporate announcement India stocks",
        ]
        news_items = fetch_combined_news(queries, max_per_query=12, total_max=40)
    else:
        news_items = fetch_google_news_rss(category_info["query"], max_results=35)

    news_items = apply_sentiment_to_news(news_items)

    return render_template(
        "news/index.html",
        news_items=news_items,
        selected_category=category,
        category_name=category_info["name"],
        category_description=category_info.get("description", ""),
        indices=get_live_indices(),
        stock_list=get_filtered_tickers(),
        stock_symbol=None,
    )


@news_bp.route("/search")
def news_search():
    """Search news by custom query"""
    query = request.args.get("q", "").strip()

    if not query:
        return render_template(
            "news/index.html",
            news_items=[],
            selected_category=None,
            category_name="Search Results",
            category_description="Enter a search term to find news",
            search_query=query,
            indices=get_live_indices(),
            stock_list=get_filtered_tickers(),
            stock_symbol=None,
        )

    search_query = f"{query} India stock market"
    news_items = fetch_google_news_rss(search_query, max_results=35)
    news_items = apply_sentiment_to_news(news_items)

    return render_template(
        "news/index.html",
        news_items=news_items,
        selected_category=None,
        category_name=f"Search: {query}",
        category_description=f"Showing results for '{query}'",
        search_query=query,
        indices=get_live_indices(),
        stock_list=get_filtered_tickers(),
        stock_symbol=None,
    )


@news_bp.route("/stock/<ticker>")
def stock_news(ticker: str):
    """Get news for a specific stock"""
    ticker = ticker.upper().strip()
    query = f"{ticker} stock NSE India news"
    news_items = fetch_google_news_rss(query, max_results=25)
    news_items = apply_sentiment_to_news(news_items)

    return render_template(
        "news/index.html",
        news_items=news_items,
        selected_category=None,
        category_name=f"{ticker} News",
        category_description=f"Latest news about {ticker}",
        stock_ticker=ticker,
        indices=get_live_indices(),
        stock_list=get_filtered_tickers(),
        stock_symbol=ticker,
    )


# ========================================================================
# API ENDPOINTS
# ========================================================================


@news_bp.route("/api/fetch")
def api_fetch_news():
    """API endpoint to fetch news (returns JSON)"""
    category = request.args.get("category", DEFAULT_CATEGORY)
    query = request.args.get("q", "").strip()

    if query:
        search_query = f"{query} India stock market"
        news_items = fetch_google_news_rss(search_query, max_results=25)
        category_name = f"Search: {query}"
    elif category == "top":
        queries = [
            "stock market India breaking news today",
            "Sensex Nifty market update",
            "share market news NSE BSE",
        ]
        news_items = fetch_combined_news(queries, max_per_query=10, total_max=30)
        category_name = "Top Stories"
    elif category in NEWS_CATEGORIES:
        category_info = NEWS_CATEGORIES[category]
        news_items = fetch_google_news_rss(category_info["query"], max_results=25)
        category_name = category_info["name"]
    else:
        return jsonify({"error": "Invalid category"}), 400

    news_items = apply_sentiment_to_news(news_items)
    return jsonify(
        {
            "success": True,
            "category": category,
            "category_name": category_name,
            "count": len(news_items),
            "news": news_items,
            "attribution": "News sourced from Google News RSS. All articles are property of their respective publishers.",
        }
    )


@news_bp.route("/api/stock/<ticker>")
def api_stock_news(ticker: str):
    """API endpoint for stock-specific news"""
    ticker = ticker.upper().strip()
    query = f"{ticker} stock NSE India news"
    news_items = fetch_google_news_rss(query, max_results=20)
    news_items = apply_sentiment_to_news(news_items)

    return jsonify(
        {
            "success": True,
            "ticker": ticker,
            "count": len(news_items),
            "news": news_items,
            "attribution": "News sourced from Google News RSS. All articles are property of their respective publishers.",
        }
    )
