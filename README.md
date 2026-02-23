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
# Process all 11 sectors (default date range 2020-01-01 to 2025-05-20)
python src/yahoo_stock_data_etl_worker.py

# Specify a custom output directory
python src/yahoo_stock_data_etl_worker.py --output-dir data/out

# Process only two sectors with a custom date range
python src/yahoo_stock_data_etl_worker.py \
    --sectors Technology,Healthcare \
    --start 2022-01-01 \
    --end 2024-12-31

# Process a custom list of tickers (grouped under sector "custom")
python src/yahoo_stock_data_etl_worker.py --tickers AAPL,MSFT,NVDA

# Load sector/ticker assignments from a CSV file
python src/yahoo_stock_data_etl_worker.py --tickers-csv data/stock_sector_list_2025.csv
```

### Outputs

For each processed sector the script creates:

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
| `--output-dir` | `data/out` | Output directory (repo-relative or absolute) |
| `--start` | `2020-01-01` | Start date for trading data |
| `--end` | `2025-05-20` | End date for trading data |
| `--sectors` | *(all)* | Comma-separated sector names to process |
| `--tickers` | — | Comma-separated tickers (overrides sector mapping) |
| `--tickers-csv` | — | CSV file with `sector` and `ticker` columns |

## 🤝 Contributing

Have suggestions for companies or sectors? Spot an error or want to help with the educational content? Pull requests and issues are welcome.
