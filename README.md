##### Note
It was a bootcamp invidiaul project used a synthetic dataset from Kaggle, which had some limitations. So after the bootcamp, I have been trying to scrap trading data from Yahoo Finance to gradually improve the project with real data.

-----------------------------------------------------------------------------------------------------------------



# 📊 Beginner-Friendly Stock Selector by Sector

Welcome to the **Beginner Stock Selector**, a curated and educational tool designed to help new investors explore the stock market, one sector at a time.

In a world where platforms like **Robinhood**, **Trade Republic**, **N26**, and **Trading 212** have made investing easier than ever, beginners face a new challenge: **too many tools, too little clarity**. This project simplifies the first step.

## 🎯 Project Objective

To provide a simple, beginner-friendly list of **20 actively traded stocks from each of the 11 GICS sectors**, ranked by 5-year trading activity. Our goal is to **educate, not overwhelm** — by guiding users through real companies, organised by sector, with context and clarity.

## 📦 What's Inside

- ✅ **11 Sectors**: Each major sector in the GICS classification
- ✅ **~20 Stocks per Sector**: Curated for liquidity, recognition, and educational value
- ✅ **219 Total Tickers**
- ✅ **Educational Context**: Coming soon — plain-language descriptions of each sector and company role
- ✅ **Data Source**: Yahoo Finance (via `yfinance` or scraping)

## 📌 Scope

- Curated, static dataset of 219 stocks across 11 sectors
- Intended for educational and exploratory use
- Uses publicly available market data from the past 5 years
- Designed for future integration into web-based tools or interactive dashboards

## 🧮 Selection Criteria

| Criterion                 | Description |
|--------------------------|-------------|
| **Sector Representation** | Stocks selected to reflect all 11 GICS sectors (e.g. Technology, Healthcare, Energy) |
| **Liquidity & Activity**  | 5-Year Trading Value = Daily Volume × Closing Price, summed over 5 years |
| **Beginner Suitability**  | Focus on large-cap or widely known companies (e.g. Apple, Microsoft, Visa) |
| **Data Reliability**      | Stocks with complete, consistent historical data on Yahoo Finance |

## 🛠 Future Plans

- 🔄 Convert this dataset into a Streamlit or Lovable-powered web tool
- 🧠 Add educational content: what is a sector, what is trading value, how to use this tool
- 📈 Enable simple charts and learning paths by sector

## 🔧 Yahoo Stock Data ETL Worker

`src/yahoo_stock_data_etl_worker.py` is a standalone batch script that downloads
trading data and company metadata from Yahoo Finance via `yfinance`, cleans them,
and writes per-sector CSV files to an output directory.

### Dependencies

```
yfinance
pandas
```

Install them with:

```bash
pip install yfinance pandas
```

### Running the worker

```bash
# Daily incremental update (last 7 days, default mode)
python src/yahoo_stock_data_etl_worker.py --mode daily

# Full historical backfill from 2020-01-01 through today
python src/yahoo_stock_data_etl_worker.py --mode backfill

# Process only two sectors with a custom date range
python src/yahoo_stock_data_etl_worker.py \
    --sectors Technology,Healthcare \
    --start 2022-01-01 \
    --end 2024-12-31

# Enable CSV file output for local debugging (off by default)
python src/yahoo_stock_data_etl_worker.py --mode daily --write-csv

# Process a custom list of tickers (grouped under sector "custom")
python src/yahoo_stock_data_etl_worker.py --tickers AAPL,MSFT,NVDA

# Load sector/ticker assignments from a CSV file
python src/yahoo_stock_data_etl_worker.py --tickers-csv data/stock_sector_list_2025.csv
```

### Outputs

For each processed sector the script creates (when `--write-csv` is passed):

| File | Description |
|------|-------------|
| `<output-dir>/<Sector>/<Sector>_trading_data.csv` | Cleaned OHLCV data with a `Ticker` column |
| `<output-dir>/<Sector>/<Sector>_metadata.csv` | Cleaned company metadata |

When more than one sector is processed, merged files are also written:

| File | Description |
|------|-------------|
| `<output-dir>/merged_trading_data.csv` | All sectors' trading data combined |
| `<output-dir>/merged_metadata.csv` | All sectors' metadata combined |

### CLI reference

| Argument | Default | Description |
|----------|---------|-------------|
| `--mode` | `daily` | `daily` fetches last 7 days; `backfill` fetches from 2020-01-01 |
| `--output-dir` | `data/out` | Output directory for CSV files (repo-relative or absolute) |
| `--start` | *(mode default)* | Start date for trading data (YYYY-MM-DD) |
| `--end` | *(today UTC)* | End date for trading data (YYYY-MM-DD) |
| `--sectors` | *(all)* | Comma-separated sector names to process |
| `--tickers` | — | Comma-separated tickers (overrides sector mapping) |
| `--tickers-csv` | — | CSV file with `sector` and `ticker` columns |
| `--write-csv` | *(off)* | Write per-sector and merged CSV files |

### Loading to Supabase / Postgres

The worker reads `DATABASE_URL` from the environment.  When set, cleaned data
is upserted into two tables:

#### `market_data.daily_prices`

| Column | Type | Notes |
|--------|------|-------|
| `ticker` | text | Primary key (with `date`) |
| `date` | date | Primary key (with `ticker`) |
| `open` | numeric | |
| `high` | numeric | |
| `low` | numeric | |
| `close` | numeric | |
| `volume` | numeric | |
| `ingested_at` | timestamptz | Set to `now()` on every upsert |

#### `market_data.ticker_metadata`

| Column | Type | Notes |
|--------|------|-------|
| `ticker` | text | Primary key |
| `sector` | text | |
| `industry` | text | |
| `marketcap` | numeric | |
| `beta` | numeric | |
| `dividendyield` | numeric | |
| `trailingpe` | numeric | |
| `forwardpe` | numeric | |
| `earningsquarterlygrowth` | numeric | |
| `fulltimeemployees` | numeric | |
| `country` | text | |
| `website` | text | |
| `ingested_at` | timestamptz | Set to `now()` on every upsert |

#### Setting up the DATABASE_URL secret

1. In your Supabase project, go to **Settings → Database** and copy the
   connection string (use the *direct* connection or a *pooler* URL).
2. Create a GitHub Actions secret called `DATABASE_URL` in your repository:
   **Settings → Secrets and variables → Actions → New repository secret**.
3. The worker automatically picks it up via `os.environ.get("DATABASE_URL")`.

To run a backfill locally:

```bash
export DATABASE_URL="postgresql://user:password@host:5432/dbname"
python src/yahoo_stock_data_etl_worker.py --mode backfill
```

### Scheduled GitHub Actions workflow

`.github/workflows/yahoo_etl.yml` runs the worker in `daily` mode every day
at **08:00 UTC** and can also be triggered manually via **workflow_dispatch**.
It requires the `DATABASE_URL` repository secret described above.

## 🤝 Contributing

Have suggestions for companies or sectors? Spot an error or want to help with the educational content? Pull requests and issues are welcome.
