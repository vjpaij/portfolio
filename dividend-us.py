import pandas as pd
import yfinance as yf
from datetime import datetime

def fetch_us_dividend_calendar(input_csv_path, output_csv_path):
    df = pd.read_csv(input_csv_path)
    df['Transaction Date'] = pd.to_datetime(df['Transaction Date'], dayfirst=True)
    df['Symbol'] = df['Symbol'].astype(str)

    fy_start = datetime(2024, 4, 1)
    fy_end = datetime(2025, 3, 31)

    latest_holdings = (
        df.sort_values(['Symbol', 'Transaction Date'])
        .groupby('Symbol')
        .last()
        .reset_index()
    )

    active_holdings = latest_holdings[latest_holdings['Total Shares'] > 0][['Symbol', 'Total Shares']]
    print(f"🟢 Found {len(active_holdings)} symbols with current holdings.")

    calendar_data = []

    for _, row in active_holdings.iterrows():
        symbol = row['Symbol']
        quantity = row['Total Shares']
        try:
            ticker = yf.Ticker(symbol)
            dividends = ticker.dividends

            if not dividends.empty:
                dividends.index = dividends.index.tz_localize(None)
                dividends = dividends[(dividends.index >= fy_start) & (dividends.index <= fy_end)]

                for date, amount in dividends.items():
                    calendar_data.append({
                        "Month": date.strftime('%B'),
                        "Month Number": (date.month if date.month >= 4 else date.month + 12),  # Apr=4 → Mar=15
                        "Dividend Date": date.strftime('%Y-%m-%d'),
                        "Symbol": symbol,
                        "Total Shares": quantity,
                        "Dividend Amount": amount
                    })
            else:
                print(f"⚠️ No dividend data found for {symbol}")
        except Exception as e:
            print(f"⚠️ Error fetching dividend for {symbol}: {e}")
            continue

    if calendar_data:
        df_calendar = pd.DataFrame(calendar_data)
        df_calendar.sort_values(by=['Month Number', 'Dividend Date', 'Symbol'], inplace=True)
        df_calendar.drop(columns='Month Number', inplace=True)

        df_calendar.to_csv(output_csv_path, index=False)
        print(f"✅ U.S. Dividend calendar written to {output_csv_path}")
    else:
        print("❌ No dividend data available for any U.S. symbols in FY 2024-25.")

# Example usage
input_csv = '/Users/in22417145/PycharmProjects/portfolio/data/us-stocks.csv'
output_csv = '/Users/in22417145/PycharmProjects/portfolio/data/dividend-calendar.csv'
fetch_us_dividend_calendar(input_csv, output_csv)
