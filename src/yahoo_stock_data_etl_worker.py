"""Yahoo Stock Data ETL Worker

Sector-by-sector download, cleaning, and merging of Yahoo Finance
trading data and company metadata.

Usage:
    python src/yahoo_stock_data_etl_worker.py [options]

See --help for all options.
"""

import argparse
import math
import os
from datetime import datetime, timedelta, timezone

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

# Mapping from DataFrame column names to database column names for metadata
METADATA_RENAME: dict[str, str] = {
    "marketCap": "marketcap",
    "dividendYield": "dividendyield",
    "trailingPE": "trailingpe",
    "forwardPE": "forwardpe",
    "earningsQuarterlyGrowth": "earningsquarterlygrowth",
    "fullTimeEmployees": "fulltimeemployees",
}


# ---------------------------------------------------------------------------
# Bigint helpers
# ---------------------------------------------------------------------------

_BIGINT_MAX = 9_223_372_036_854_775_807
_BIGINT_MIN = -9_223_372_036_854_775_808


def _sanitize_bigint_series(series: pd.Series) -> pd.Series:
    """Coerce *series* to nullable integers safe for a PostgreSQL ``bigint`` column.

    Non-finite floats (NaN, ±inf) and values outside the signed 64-bit range
    are replaced with ``None``; otherwise values are truncated to ``int``.
    Prints a message when values are coerced so the workflow output stays
    readable without being flooded.
    """
    def _coerce(v):
        if v is None:
            return None
        try:
            f = float(v)
        except (TypeError, ValueError):
            return None
        if not math.isfinite(f):
            return None
        i = int(f)
        if not (_BIGINT_MIN <= i <= _BIGINT_MAX):
            return None
        return i

    coerced_list = [_coerce(v) for v in series]
    coerced = pd.Series(coerced_list, index=series.index, name=series.name, dtype=object)
    dropped = int(series.notna().sum()) - int(coerced.notna().sum())
    if dropped > 0:
        print(
            f"  Coerced {dropped} out-of-range/non-finite value(s) in "
            f"'{series.name}' to NULL (bigint overflow guard)"
        )
    return coerced


def _to_records_nullsafe(df: pd.DataFrame) -> list[dict]:
    """Convert *df* to a list of dicts, replacing non-finite floats with ``None``.

    ``pd.DataFrame.to_dict(orient='records')`` preserves ``float('nan')`` in
    float64 columns even after ``df.where(pd.notna(df), None)``.  This helper
    ensures that NaN / ±inf values are mapped to ``None`` so the DB driver can
    insert SQL ``NULL``.
    """
    return [
        {
            k: (None if isinstance(v, float) and not math.isfinite(v) else v)
            for k, v in row.items()
        }
        for row in df.to_dict(orient="records")
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
    - Normalize common missing-value sentinels (``"NaN"``, ``"nan"``,
      ``"None"``, ``"null"``, ``""``) to ``None`` so they are stored as SQL
      ``NULL`` rather than literal strings.
    - Drop rows where *all* columns except ``ticker`` are ``NaN``/``None``.
    - Coerce numeric metadata columns.
    """
    if df.empty:
        return df

    non_ticker_cols = [c for c in df.columns if c != "ticker"]
    sentinels = {"NaN", "nan", "None", "null", ""}
    df[non_ticker_cols] = df[non_ticker_cols].replace(list(sentinels), None)
    df = df.dropna(subset=non_ticker_cols, how="all").reset_index(drop=True)
    for col in NUMERIC_META_COLS:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")

    return df


# ---------------------------------------------------------------------------
# Database loading
# ---------------------------------------------------------------------------

def _make_engine(database_url: str):
    """Create a SQLAlchemy engine from *database_url*."""
    try:
        from sqlalchemy import create_engine  # noqa: PLC0415
    except ImportError as exc:
        raise ImportError(
            "sqlalchemy is required for DB loading. "
            "Install it with: pip install sqlalchemy psycopg2-binary"
        ) from exc
    return create_engine(database_url)


def upsert_trading_data(df: pd.DataFrame, engine) -> None:
    """Upsert cleaned trading data into ``market_data.daily_prices``.

    Maps DataFrame columns to lowercase DB columns and performs an idempotent
    ``INSERT … ON CONFLICT (ticker, date) DO UPDATE`` so that re-runs do not
    create duplicate rows.  ``ingested_at`` is set to ``now()`` on every
    insert or update.
    """
    if df.empty:
        return

    from sqlalchemy import text  # noqa: PLC0415

    db_df = df[["Date", "Ticker", "Open", "High", "Low", "Close", "Volume"]].copy()
    db_df = db_df.rename(columns={
        "Date": "date",
        "Ticker": "ticker",
        "Open": "open",
        "High": "high",
        "Low": "low",
        "Close": "close",
        "Volume": "volume",
    })
    # Sanitize the bigint column before the NULL sweep so out-of-range / NaN
    # floats become Python None (-> SQL NULL) rather than raising an overflow.
    db_df["volume"] = _sanitize_bigint_series(db_df["volume"])
    # Replace remaining NaN/NaT with None so the DB driver maps them to SQL NULL.
    # Note: db_df.where() alone does not convert NaN in float64 columns to None
    # when the result is converted to dicts, so we use _to_records_nullsafe.
    db_df = db_df.where(pd.notna(db_df), None)

    sql = text("""
        INSERT INTO market_data.daily_prices
            (date, ticker, open, high, low, close, volume, ingested_at)
        VALUES (:date, :ticker, :open, :high, :low, :close, :volume, now())
        ON CONFLICT (ticker, date) DO UPDATE SET
            open        = EXCLUDED.open,
            high        = EXCLUDED.high,
            low         = EXCLUDED.low,
            close       = EXCLUDED.close,
            volume      = EXCLUDED.volume,
            ingested_at = now()
    """)
    records = _to_records_nullsafe(db_df)
    with engine.begin() as conn:
        conn.execute(sql, records)
    print(f"  Upserted {len(records)} rows into market_data.daily_prices")


def upsert_metadata(df: pd.DataFrame, engine) -> None:
    """Upsert cleaned metadata into ``market_data.ticker_metadata``.

    Maps camelCase DataFrame columns to lowercase DB columns and performs an
    idempotent ``INSERT … ON CONFLICT (ticker) DO UPDATE``.
    ``ingested_at`` is refreshed on every update.
    """
    if df.empty:
        return

    from sqlalchemy import text  # noqa: PLC0415

    db_df = df.rename(columns=METADATA_RENAME).copy()
    # Sanitize bigint columns before the NULL sweep so that out-of-range /
    # non-finite floats become Python None (→ SQL NULL) rather than raising an
    # overflow or "invalid input syntax" error in the DB driver.
    for _bigint_col in ("marketcap", "fulltimeemployees"):
        if _bigint_col in db_df.columns:
            db_df[_bigint_col] = _sanitize_bigint_series(db_df[_bigint_col])
    # Replace remaining NaN/NaT with None; use _to_records_nullsafe for the
    # records dict because pandas float64 columns keep NaN after .where().
    db_df = db_df.where(pd.notna(db_df), None)

    # Validate column names against the known allowlist to prevent SQL injection.
    # Columns originate from META_FIELDS (hardcoded) renamed via METADATA_RENAME.
    _allowed_meta_cols = {
        "ticker", "sector", "industry", "marketcap", "beta", "dividendyield",
        "trailingpe", "forwardpe", "earningsquarterlygrowth", "fulltimeemployees",
        "country", "website",
    }
    cols = db_df.columns.tolist()
    unknown = set(cols) - _allowed_meta_cols
    if unknown:
        raise ValueError(f"Unexpected metadata columns (possible injection risk): {unknown}")
    col_names = ", ".join(cols)
    placeholders = ", ".join(f":{c}" for c in cols)
    update_set = ", ".join(
        f"{c} = EXCLUDED.{c}" for c in cols if c != "ticker"
    )
    sql = text(f"""
        INSERT INTO market_data.ticker_metadata ({col_names}, ingested_at)
        VALUES ({placeholders}, now())
        ON CONFLICT (ticker) DO UPDATE SET
            {update_set},
            ingested_at = now()
    """)
    records = _to_records_nullsafe(db_df)
    with engine.begin() as conn:
        conn.execute(sql, records)
    print(f"  Upserted {len(records)} rows into market_data.ticker_metadata")

# ---------------------------------------------------------------------------
# Sector runner
# ---------------------------------------------------------------------------

def run_sector(
    sector: str,
    tickers: list[str],
    output_dir: str,
    start: str,
    end: str,
    write_csv: bool = True,
    engine=None,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Run the full ETL pipeline for one sector.

    When *write_csv* is ``True`` (default), creates ``<output_dir>/<sector>/``
    and writes per-sector CSV files.  When *engine* is provided, cleaned data
    is upserted into the Supabase/Postgres database.

    Returns the cleaned (trading_df, meta_df) tuple.
    """
    print(f"\n=== Processing sector: {sector} ===")

    # Pre-create the sector output directory when CSV writing is enabled
    if write_csv:
        sector_dir = os.path.join(output_dir, sector)
        os.makedirs(sector_dir, exist_ok=True)
    else:
        sector_dir = ""

    # Trading data
    trading_df = fetch_trading_data(tickers, start, end)
    trading_df = clean_trading_data(trading_df)
    if not trading_df.empty:
        if write_csv:
            trading_path = os.path.join(sector_dir, f"{sector}_trading_data.csv")
            trading_df.to_csv(trading_path, index=False)
            print(f"  Saved trading data -> {trading_path}")
        if engine is not None:
            try:
                upsert_trading_data(trading_df, engine)
            except Exception as exc:
                print(f"  WARNING: Failed to upsert trading data for {sector}: {exc}")

    # Metadata
    meta_df = fetch_metadata(tickers)
    meta_df = clean_metadata(meta_df)
    if not meta_df.empty:
        if write_csv:
            meta_path = os.path.join(sector_dir, f"{sector}_metadata.csv")
            meta_df.to_csv(meta_path, index=False)
            print(f"  Saved metadata      -> {meta_path}")
        if engine is not None:
            try:
                upsert_metadata(meta_df, engine)
            except Exception as exc:
                print(f"  WARNING: Failed to upsert metadata for {sector}: {exc}")

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
        "--mode",
        choices=["daily", "backfill"],
        default="daily",
        help=(
            "Run mode: 'daily' fetches the last 7 calendar days (for incremental "
            "updates); 'backfill' fetches from 2020-01-01 through today.  "
            "Explicit --start/--end values always override mode defaults."
        ),
    )
    parser.add_argument(
        "--output-dir",
        default="data/out",
        help="Directory for output files (relative to repo root, or absolute).",
    )
    parser.add_argument(
        "--start",
        default=None,
        help=(
            "Start date for trading data (YYYY-MM-DD).  "
            "Defaults to mode-specific value when not provided."
        ),
    )
    parser.add_argument(
        "--end",
        default=None,
        help=(
            "End date for trading data (YYYY-MM-DD).  "
            "Defaults to today's UTC date when not provided."
        ),
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
    parser.add_argument(
        "--write-csv",
        action="store_true",
        default=False,
        help=(
            "Write per-sector and merged CSV files to --output-dir.  "
            "Off by default; enable for local debugging."
        ),
    )
    return parser


def main() -> None:
    parser = _build_parser()
    args = parser.parse_args()

    # Resolve today's UTC date once
    today_date = datetime.now(tz=timezone.utc).date()
    today = today_date.isoformat()

    # Apply mode-specific defaults for start/end when not explicitly provided
    if args.mode == "backfill":
        start = args.start or "2020-01-01"
    else:  # daily
        start = args.start or (today_date - timedelta(days=7)).isoformat()
    end = args.end or today

    # Resolve output directory relative to the repo root when not absolute
    repo_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    output_dir = (
        args.output_dir
        if os.path.isabs(args.output_dir)
        else os.path.join(repo_root, args.output_dir)
    )
    if args.write_csv:
        os.makedirs(output_dir, exist_ok=True)
        print(f"Output directory: {output_dir}")

    # Set up DB engine if DATABASE_URL is available
    database_url = os.environ.get("DATABASE_URL")
    engine = None
    if database_url:
        print("DATABASE_URL found – testing DB connection...")
        try:
            from sqlalchemy import text as _text  # noqa: PLC0415
            engine = _make_engine(database_url)
            with engine.connect() as conn:
                conn.execute(_text("SELECT 1"))
            print("DB connection successful – DB loading enabled.")
        except ImportError:
            raise
        except Exception as exc:
            print(f"WARNING: DB connection failed: {exc}")
            print("Skipping DB loading; ETL will run without writing to the database.")
            engine = None
    else:
        print("DATABASE_URL not set – skipping DB loading.")

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

    print(f"Mode: {args.mode}")
    print(f"Sectors to process: {', '.join(sector_map.keys())}")
    print(f"Date range: {start} to {end}\n")

    all_trading: list[pd.DataFrame] = []
    all_meta: list[pd.DataFrame] = []

    for sector, tickers in sector_map.items():
        trading_df, meta_df = run_sector(
            sector, tickers, output_dir, start, end,
            write_csv=args.write_csv,
            engine=engine,
        )
        if not trading_df.empty:
            all_trading.append(trading_df)
        if not meta_df.empty:
            all_meta.append(meta_df)

    # Write merged CSV outputs when more than one sector was processed
    if args.write_csv and len(all_trading) > 1:
        merged_trading = pd.concat(all_trading, ignore_index=True)
        merged_trading_path = os.path.join(output_dir, "merged_trading_data.csv")
        merged_trading.to_csv(merged_trading_path, index=False)
        print(f"\nSaved merged trading data -> {merged_trading_path}")

    if args.write_csv and len(all_meta) > 1:
        merged_meta = pd.concat(all_meta, ignore_index=True)
        merged_meta_path = os.path.join(output_dir, "merged_metadata.csv")
        merged_meta.to_csv(merged_meta_path, index=False)
        print(f"Saved merged metadata      -> {merged_meta_path}")

    print("\nETL complete.")


if __name__ == "__main__":
    main()
