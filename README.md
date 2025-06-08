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

## 🤝 Contributing

Have suggestions for companies or sectors? Spot an error or want to help with the educational content? Pull requests and issues are welcome.
