# Extract the data from Yahoo Finance

import os
import yfinance as yf
import pandas as pd

def download_trading_data(tickers, save_path, start="2020-01-01", end="2025-05-20"):
    os.makedirs(save_path, exist_ok=True)
    for ticker in tickers:
        print(f"Downloading {ticker}...")
        try:
            df = yf.download(ticker, start=start, end=end)
            if not df.empty:
                df.to_csv(os.path.join(save_path, f"{ticker}.csv"))
                print(f"Saved: {ticker}.csv")
            else:
                print(f"No data for {ticker}")
        except Exception as e:
            print(f"Error downloading {ticker}: {e}")
            

def fetch_metadata(tickers, save_path):
    metadata_list = []
    for ticker in tickers:
        try:
            info = yf.Ticker(ticker).info
            metadata_list.append({
                "ticker": ticker,
                "sector": info.get("sector"),
                "industry": info.get("industry"),
                "marketCap": info.get("marketCap"),
                "beta": info.get("beta"),
                "dividendYield": info.get("dividendYield"),
                "trailingPE": info.get("trailingPE"),
                "forwardPE": info.get("forwardPE"),
                "earningsQuarterlyGrowth": info.get("earningsQuarterlyGrowth"),
                "fullTimeEmployees": info.get("fullTimeEmployees"),
                "country": info.get("country"),
                "website": info.get("website")
            })
        except Exception as e:
            print(f"Error retrieving data for {ticker}: {e}")

    df = pd.DataFrame(metadata_list)
    os.makedirs(os.path.dirname(save_path), exist_ok=True)
    df.to_csv(save_path, index=False)
    print(f"Saved metadata to {save_path}")
    
    
   
   
   
   


# Transform and clean raw data
def clean_trading_data(df):
    df = df[~df['Price'].isin(['Ticker', 'Date', 'Close', 'High', 'Low', 'Open', 'Volume'])].copy()
    df['Price'] = pd.to_datetime(df['Price'], errors='coerce')
    df.rename(columns={'Price': 'Date'}, inplace=True)
    df = df.dropna(subset=['Date'])
    numeric_cols = ['Close', 'High', 'Low', 'Open', 'Volume']
    for col in numeric_cols:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors='coerce')
    return df.reset_index(drop=True)


   
   
  
  
# Load and consolidate
def merge_csvs(folder_path, keyword, output_file):
    files = [f for f in os.listdir(folder_path) if keyword in f.lower()]
    dfs = [pd.read_csv(os.path.join(folder_path, f)) for f in files]
    merged = pd.concat(dfs, ignore_index=True)
    merged.to_csv(os.path.join(folder_path, output_file), index=False)
    print(f"Merged CSV saved to {output_file}")
    return merged







def run_etl_pipeline(tickers, base_folder, sector_name, start="2020-01-01", end="2025-05-20"):
    import os
    import pandas as pd

    print(f"🚀 Starting ETL pipeline for sector: {sector_name}")

    # 1. Define paths
    sector_folder = os.path.join(base_folder, sector_name)
    trading_path = os.path.join(sector_folder, "trading_data")
    metadata_path = os.path.join(sector_folder, f"{sector_name}_metadata.csv")
    merged_path = os.path.join(sector_folder, f"{sector_name}_merged_trading_data.csv")

    # 2. Download trading data
    download_trading_data(tickers, trading_path, start, end)

    # 3. Fetch and save metadata
    fetch_metadata(tickers, metadata_path)

    # 4. Sector validation
    metadata_df = pd.read_csv(metadata_path)
    mismatches = metadata_df[metadata_df['sector'].str.lower() != sector_name.lower()]
    if not mismatches.empty:
        print("⚠️ Sector mismatch detected for the following tickers:")
        print(mismatches[['ticker', 'sector']])
        print("🚫 ETL stopped. Please verify your ticker list or sector_name.")
        return None

    # 5. Merge trading CSVs
    merged_df = merge_csvs(trading_path, keyword=".csv", output_file=f"{sector_name}_merged_trading_data.csv")

    # 6. Clean merged trading data
    cleaned_df = clean_trading_data(merged_df)

    # 7. Save cleaned trading data
    cleaned_path = os.path.join(sector_folder, f"{sector_name}_cleaned_trading_data.csv")
    cleaned_df.to_csv(cleaned_path, index=False)

    print(f"✅ ETL completed for sector: {sector_name}")
    return cleaned_df
