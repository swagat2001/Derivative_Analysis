# Cash Data Pipeline

## Overview

This pipeline downloads and manages Cash/Equity market data from NSE, similar to how the FO pipeline manages F&O data.

## Database

**Database Name:** `CashStocks_Database`
**Main Table:** `TBL_BHAVCOPY_CASH`

## Usage

```bash
cd c:\Users\Admin\Desktop\Derivative_Analysis\Database\Cash
python cash_update_database.py
```

## What it does

1. **Downloads** Cash Bhavcopy files from NSE (`sec_bhavdata_full_DDMMYYYY.csv`)
2. **Uploads** data to `TBL_BHAVCOPY_CASH` table in `CashStocks_Database`

## Data Flow

```
NSE Website
    ↓
Download CSV (sec_bhavdata_full_*.csv)
    ↓
Save to C:/NSE_EOD_CASH
    ↓
Parse and Upload to PostgreSQL
    ↓
TBL_BHAVCOPY_CASH table
    ↓
Insights Dashboard (/insights/)
```

## Table Columns

| Column | Description |
|--------|------------|
| BizDt | Business/Trading Date |
| TckrSymb | Ticker Symbol |
| OpnPric | Open Price |
| HghPric | High Price |
| LwPric | Low Price |
| ClsPric | Close Price |
| TtlTradgVol | Total Trading Volume |
| TtlTrfVal | Total Traded Value (Turnover) |
| DlvryQty | Delivery Quantity |

## Files

- `cash_update_database.py` - Main pipeline script (download + upload)
- `cash_complete_pipeline.py` - Original download-only script (kept for reference)
