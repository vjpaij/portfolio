import pandas as pd
import yfinance as yf
from datetime import datetime, timedelta

def fetch_dividends_for_current_holdings(input_csv_path, output_csv_path):
    df = pd.read_csv(input_csv_path)
    df['Transaction Date'] = pd.to_datetime(df['Transaction Date'], dayfirst=True)
    df['Symbol'] = df['Symbol'].astype(str)

    today = datetime.today()
    one_year_ago = today - timedelta(days=365)

    # Get latest holding per symbol
    latest_holdings = (
        df.sort_values(['Symbol', 'Transaction Date'])
        .groupby('Symbol')
        .last()
        .reset_index()
    )

    # Filter symbols with Total Shares > 0
    active_holdings = latest_holdings[latest_holdings['Total Shares'] > 0][['Symbol', 'Total Shares']]
    print(f"🟢 Found {len(active_holdings)} symbols with current holdings.")

    dividends_data = []

    for _, row in active_holdings.iterrows():
        symbol = row['Symbol']
        quantity = row['Total Shares']
        dividends_found = False

        for suffix in [".NS", ".BO"]:
            try:
                full_symbol = symbol + suffix
                ticker = yf.Ticker(full_symbol)
                dividends = ticker.dividends

                if not dividends.empty:
                    # Remove timezone from index before filtering
                    dividends.index = dividends.index.tz_localize(None)
                    dividends = dividends[dividends.index >= one_year_ago]

                    for date, amount in dividends.items():
                        dividends_data.append({
                            "Dividend Date": date.strftime('%Y-%m-%d'),
                            "Symbol": symbol,
                            "Total Shares": quantity,
                            "Dividend Amount": amount
                        })
                    dividends_found = True
                    break  # Stop after first found (prefer NSE)
            except Exception as e:
                print(f"⚠️ Error fetching dividend for {symbol}{suffix}: {e}")
                continue

        if not dividends_found:
            print(f"⚠️ No dividend data found for {symbol} on NSE or BSE")

    # Convert to DataFrame
    if dividends_data:
        df_dividends = pd.DataFrame(dividends_data)
        df_dividends.sort_values(by=['Dividend Date', 'Symbol'], inplace=True)
        df_dividends.to_csv(output_csv_path, index=False)
        print(f"✅ Dividend data written to {output_csv_path}")
    else:
        print("❌ No dividend data available for any symbols.")

# Example usage
input_csv = '/Users/in22417145/PycharmProjects/portfolio/data/ind-stocks.csv'
output_csv = '/Users/in22417145/PycharmProjects/portfolio/data/ind-dividend.csv'
fetch_dividends_for_current_holdings(input_csv, output_csv)
