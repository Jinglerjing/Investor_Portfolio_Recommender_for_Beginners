"""Yahoo Stock Data ETL Worker

Sector-by-sector download, cleaning, and merging of Yahoo Finance
trading data and company metadata.

Usage:
    python src/yahoo_stock_data_etl_worker.py [options]

See --help for all options.
"""

import argparse
import os

import pandas as pd
import yfinance as yf


# ---------------------------------------------------------------------------
# Default sector -> ticker mapping (sourced from the ETL notebook)
# ---------------------------------------------------------------------------

SECTOR_TICKERS: dict[str, list[str]] = {
    "Technology": [
        "AAPL", "MSFT", "AMD", "ADBE", "AVGO", "INTC", "CRM", "CSCO",
        "NVDA", "ORCL", "QCOM", "PLTR", "IBM", "NOW", "TXN", "CDNS",
        "ASML", "GFS", "APP", "ANSS",
    ],
    "Healthcare": [
        "ABBV", "ABT", "AMGN", "JNJ", "PFE", "LLY", "MRK", "GEHC",
        "ISRG", "MDT", "DHR", "GILD", "BIIB", "DXCM", "IDXX", "BMY",
        "TMO", "AZN", "HCA", "CVS",
    ],
    "Financials": [
        "AIG", "AXP", "BAC", "BK", "C", "GS", "JPM", "MA", "MET", "MS",
        "PYPL", "SCHW", "USB", "V", "BRK-B", "COF", "BLK", "TROW",
        "PGR", "FITB",
    ],
    "Consumer_Cyclical": [
        "AMZN", "TSLA", "HD", "MCD", "BKNG", "GM", "LOW", "NKE", "SBUX",
        "TGT", "ABNB", "YUM", "ROST", "TJX", "ULTA", "F", "EBAY",
        "ETSY", "BBY",
    ],
    "Consumer_Defensive": [
        "CL", "COST", "KO", "MDLZ", "MO", "PG", "PEP", "WMT", "KDP",
        "CLX", "KR", "UNFI", "GIS", "TSN", "SYY", "ADM", "KMB", "EL",
        "HSY", "CHD",
    ],
    "Energy": [
        "COP", "CVX", "XOM", "FANG", "BKR", "EOG", "OXY", "PSX", "HAL",
        "SLB", "VLO", "HES", "DVN", "MPC", "APA", "CPE", "SM", "MTDR",
        "PDCE", "PXD",
    ],
    "Industrials": [
        "BA", "CAT", "DE", "EMR", "FDX", "GD", "HON", "LMT", "UNP",
        "UPS", "CSX", "ADP", "MMM", "RTX", "GE", "SWK", "ITW", "ETN",
        "IR", "PCAR",
    ],
    "Communication_Services": [
        "DIS", "GOOG", "GOOGL", "META", "CMCSA", "T", "TMUS", "CHTR",
        "NFLX", "VZ", "SIRI", "PARA", "FOXA", "WBD", "TTWO", "ATVI",
        "LYV", "BIDU", "NTES", "SPOT",
    ],
    "Utilities": [
        "DUK", "NEE", "SO", "EXC", "CEG", "XEL", "AEP", "ES", "D",
        "NRG", "PPL", "PEG", "ED", "EVRG", "EIX", "WEC", "AWK", "ATO",
        "SRE", "CMS",
    ],
    "Real_Estate": [
        "AMT", "CSGP", "SPG", "WELL", "O", "PLD", "BXP", "EQIX", "PSA",
        "EQR", "VNO", "SLG", "AVB", "FRT", "OHI", "DLR", "HST", "WY",
        "IRM", "ARE",
    ],
    "Basic_Materials": [
        "LIN", "DD", "ECL", "FCX", "NEM", "APD", "MOS", "PPG", "RPM",
        "CE", "EMN", "ALB", "VMC", "CF", "TREX", "MLM", "IFF", "NUE",
        "AVY", "BALL",
    ],
}

# Metadata fields fetched from yfinance Ticker.info
META_FIELDS = [
    "ticker", "sector", "industry", "marketCap", "beta", "dividendYield",
    "trailingPE", "forwardPE", "earningsQuarterlyGrowth",
    "fullTimeEmployees", "country", "website",
]

# Metadata columns that should be numeric
NUMERIC_META_COLS = [
    "marketCap", "beta", "dividendYield", "trailingPE", "forwardPE",
    "earningsQuarterlyGrowth", "fullTimeEmployees",
]


# ---------------------------------------------------------------------------
# ETL functions
# ---------------------------------------------------------------------------

def fetch_trading_data(tickers: list[str], start: str, end: str) -> pd.DataFrame:
    """Download OHLCV trading data for *tickers* and return a merged DataFrame.

    A ``Ticker`` column is added to each row so that the combined DataFrame
    can be linked back to individual companies.  The ``Date`` column is a
    proper ``datetime64`` column (not the index).
    """
    frames = []
    for ticker in tickers:
        print(f"  Downloading trading data for {ticker}...")
        try:
            df = yf.download(ticker, start=start, end=end, progress=False)
            if df.empty:
                print(f"  No data returned for {ticker}")
                continue
            # yfinance >=0.2 returns MultiIndex columns for a single ticker
            # (e.g. ('Close', 'AAPL')).  Flatten to simple column names.
            if isinstance(df.columns, pd.MultiIndex):
                df.columns = df.columns.droplevel(1)
            df = df.reset_index()  # DatetimeIndex -> 'Date' column
            df["Ticker"] = ticker
            frames.append(df)
        except Exception as exc:
            print(f"  Error downloading {ticker}: {exc}")

    if not frames:
        return pd.DataFrame()
    return pd.concat(frames, ignore_index=True)


def clean_trading_data(df: pd.DataFrame) -> pd.DataFrame:
    """Clean a merged trading DataFrame.

    Steps:
    - If the date column is named ``Price`` (older yfinance CSV exports),
      remove injected header rows and rename it to ``Date``.
    - Ensure ``Date`` is ``datetime64``.
    - Drop rows with an invalid / missing ``Date``.
    - Coerce OHLCV columns to numeric.
    """
    if df.empty:
        return df

    # Handle older yfinance CSV exports where the date column is called 'Price'
    if "Price" in df.columns and "Date" not in df.columns:
        header_keywords = {"Ticker", "Date", "Close", "High", "Low", "Open", "Volume"}
        df = df[~df["Price"].isin(header_keywords)].copy()
        df["Price"] = pd.to_datetime(df["Price"], errors="coerce")
        df.rename(columns={"Price": "Date"}, inplace=True)

    if "Date" in df.columns:
        df["Date"] = pd.to_datetime(df["Date"], errors="coerce")
        df = df.dropna(subset=["Date"])

    numeric_cols = ["Close", "High", "Low", "Open", "Volume"]
    for col in numeric_cols:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")

    return df.reset_index(drop=True)


def fetch_metadata(tickers: list[str]) -> pd.DataFrame:
    """Fetch ``Ticker.info`` metadata for *tickers* and return a DataFrame."""
    records = []
    for ticker in tickers:
        print(f"  Fetching metadata for {ticker}...")
        try:
            info = yf.Ticker(ticker).info
            record = {field: info.get(field) for field in META_FIELDS if field != "ticker"}
            record["ticker"] = ticker
            records.append(record)
        except Exception as exc:
            print(f"  Error fetching metadata for {ticker}: {exc}")

    if not records:
        return pd.DataFrame(columns=META_FIELDS)
    return pd.DataFrame(records, columns=META_FIELDS)


def clean_metadata(df: pd.DataFrame) -> pd.DataFrame:
    """Clean a metadata DataFrame.

    Steps:
    - Drop rows where *all* columns except ``ticker`` are ``NaN``.
    - Fill remaining ``NaN`` values with ``"NaN"`` (string sentinel).
    - Coerce numeric metadata columns.
    """
    if df.empty:
        return df

    non_ticker_cols = [c for c in df.columns if c != "ticker"]
    df = df.dropna(subset=non_ticker_cols, how="all").reset_index(drop=True)
    df[non_ticker_cols] = df[non_ticker_cols].fillna("NaN")
    for col in NUMERIC_META_COLS:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")

    return df


# ---------------------------------------------------------------------------
# Sector runner
# ---------------------------------------------------------------------------

def run_sector(
    sector: str,
    tickers: list[str],
    output_dir: str,
    start: str,
    end: str,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Run the full ETL pipeline for one sector.

    Creates ``<output_dir>/<sector>/`` and writes:
    - ``<sector>_trading_data.csv``
    - ``<sector>_metadata.csv``

    Returns the cleaned (trading_df, meta_df) tuple.
    """
    print(f"\n=== Processing sector: {sector} ===")
    sector_dir = os.path.join(output_dir, sector)
    os.makedirs(sector_dir, exist_ok=True)

    # Trading data
    trading_df = fetch_trading_data(tickers, start, end)
    trading_df = clean_trading_data(trading_df)
    if not trading_df.empty:
        trading_path = os.path.join(sector_dir, f"{sector}_trading_data.csv")
        trading_df.to_csv(trading_path, index=False)
        print(f"  Saved trading data -> {trading_path}")

    # Metadata
    meta_df = fetch_metadata(tickers)
    meta_df = clean_metadata(meta_df)
    if not meta_df.empty:
        meta_path = os.path.join(sector_dir, f"{sector}_metadata.csv")
        meta_df.to_csv(meta_path, index=False)
        print(f"  Saved metadata      -> {meta_path}")

    return trading_df, meta_df


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=(
            "Download and clean Yahoo Finance trading data and metadata "
            "sector-by-sector, writing outputs to an output directory."
        ),
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    parser.add_argument(
        "--output-dir",
        default="data/out",
        help="Directory for output files (relative to repo root, or absolute).",
    )
    parser.add_argument(
        "--start",
        default="2020-01-01",
        help="Start date for trading data (YYYY-MM-DD).",
    )
    parser.add_argument(
        "--end",
        default="2025-05-20",
        help="End date for trading data (YYYY-MM-DD).",
    )
    parser.add_argument(
        "--sectors",
        default=None,
        help=(
            "Comma-separated list of sectors to process. "
            "Defaults to all sectors in the built-in mapping."
        ),
    )
    parser.add_argument(
        "--tickers",
        default=None,
        help=(
            "Comma-separated list of tickers to use instead of the built-in "
            "sector mapping.  All tickers are grouped under the sector name "
            "\"custom\"."
        ),
    )
    parser.add_argument(
        "--tickers-csv",
        default=None,
        help=(
            "Path to a CSV file with at least 'sector' and 'ticker' columns. "
            "Overrides the built-in sector mapping when provided."
        ),
    )
    return parser


def main() -> None:
    parser = _build_parser()
    args = parser.parse_args()

    # Resolve output directory relative to the repo root when not absolute
    repo_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    output_dir = (
        args.output_dir
        if os.path.isabs(args.output_dir)
        else os.path.join(repo_root, args.output_dir)
    )
    os.makedirs(output_dir, exist_ok=True)
    print(f"Output directory: {output_dir}")

    # Build sector -> tickers mapping
    if args.tickers:
        sector_map: dict[str, list[str]] = {
            "custom": [t.strip() for t in args.tickers.split(",") if t.strip()]
        }
    elif args.tickers_csv:
        csv_df = pd.read_csv(args.tickers_csv)
        csv_df.columns = [c.lower().strip() for c in csv_df.columns]
        sector_map = (
            csv_df.groupby("sector")["ticker"].apply(list).to_dict()
        )
    else:
        sector_map = dict(SECTOR_TICKERS)

    # Filter to requested sectors
    if args.sectors:
        wanted = {s.strip() for s in args.sectors.split(",") if s.strip()}
        sector_map = {k: v for k, v in sector_map.items() if k in wanted}
        if not sector_map:
            print(
                f"No matching sectors found.  Available: "
                f"{', '.join(SECTOR_TICKERS.keys())}"
            )
            return

    print(f"Sectors to process: {', '.join(sector_map.keys())}")
    print(f"Date range: {args.start} to {args.end}\n")

    all_trading: list[pd.DataFrame] = []
    all_meta: list[pd.DataFrame] = []

    for sector, tickers in sector_map.items():
        trading_df, meta_df = run_sector(
            sector, tickers, output_dir, args.start, args.end
        )
        if not trading_df.empty:
            all_trading.append(trading_df)
        if not meta_df.empty:
            all_meta.append(meta_df)

    # Write merged outputs when more than one sector was processed
    if len(all_trading) > 1:
        merged_trading = pd.concat(all_trading, ignore_index=True)
        merged_trading_path = os.path.join(output_dir, "merged_trading_data.csv")
        merged_trading.to_csv(merged_trading_path, index=False)
        print(f"\nSaved merged trading data -> {merged_trading_path}")

    if len(all_meta) > 1:
        merged_meta = pd.concat(all_meta, ignore_index=True)
        merged_meta_path = os.path.join(output_dir, "merged_metadata.csv")
        merged_meta.to_csv(merged_meta_path, index=False)
        print(f"Saved merged metadata      -> {merged_meta_path}")

    print("\nETL complete.")


if __name__ == "__main__":
    main()
