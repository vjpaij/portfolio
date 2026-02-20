'''
For Indian Stocks
'''

import os
import pandas as pd
import yfinance as yf
from datetime import datetime, timedelta


import os

def get_manual_price_history(symbol, start_date, end_date, manual_data_dir):
    """
    Fallback function: Reads manual CSV with Date and Price.
    Assumes price=0 for dates beyond available range.
    """
    file_path = os.path.join(manual_data_dir, f"{symbol}.csv")
    
    if not os.path.exists(file_path):
        return None
    
    try:
        df = pd.read_csv(file_path)
        df['Date'] = pd.to_datetime(df['Date'])
        df = df.rename(columns={'Date': 'Transaction Date', 'Price': 'Price'})
        df = df[['Transaction Date', 'Price']]
        
        # Ensure full date range with 0 for future dates
        full_dates = pd.date_range(start=start_date, end=end_date)
        full_df = pd.DataFrame({'Transaction Date': full_dates})
        merged = pd.merge(full_df, df, on='Transaction Date', how='left')
        
        # Fill forward known values; then set any remaining (future) as 0
        merged['Price'] = merged['Price'].ffill().fillna(0)
        return merged
    except Exception as e:
        print(f"❌ Failed to load manual CSV for {symbol}: {str(e)}")
        return None


def get_best_price_history(symbol_base, start_date, end_date):
    """
    Tries to fetch data for both NSE (.NS) and BSE (.BO) variants of a symbol.
    Returns the DataFrame with the highest available closing price per day.
    """
    variants = [symbol_base + ".NS", symbol_base + ".BO"]
    data_frames = []
    
    for variant in variants:
        try:
            ticker = yf.Ticker(variant)
            hist = ticker.history(start=start_date, end=end_date, auto_adjust=False)
            if hist.empty:
                print(f"No data for {variant}")
                continue
            hist = hist.reset_index()
            hist['Transaction Date'] = pd.to_datetime(hist['Date']).dt.date
            hist = hist[['Transaction Date', 'Close']]
            hist.rename(columns={'Close': f'Price_{variant[-2:]}'}, inplace=True)
            data_frames.append(hist)
        except Exception as e:
            print(f"Error fetching {variant}: {str(e)}")
            continue

    if not data_frames:
        return None

    # Merge available data on Transaction Date
    merged = data_frames[0]
    for df in data_frames[1:]:
        merged = pd.merge(merged, df, on='Transaction Date', how='outer')

    # Replace NaN with 0 before max (we’ll re-mask later)
    price_cols = [col for col in merged.columns if col.startswith("Price_")]
    merged['Price'] = merged[price_cols].max(axis=1, skipna=True)

    # Keep only Transaction Date and final Price
    final = merged[['Transaction Date', 'Price']].copy()
    final['Transaction Date'] = pd.to_datetime(final['Transaction Date'])

    # Forward fill price for holidays
    final.sort_values('Transaction Date', inplace=True)
    final['Price'].ffill(inplace=True)

    return final

def get_portfolio_values(input_csv_path, output_csv_path):
    manual_data_dir = '/Users/in22417145/PycharmProjects/portfolio/data'
    df_transactions = pd.read_csv(input_csv_path)
    df_transactions['Symbol'] = df_transactions['Symbol'].astype(str)
    df_transactions['Transaction Date'] = pd.to_datetime(df_transactions['Transaction Date'], dayfirst=True)
    symbols = df_transactions['Symbol'].unique()

    final_df = pd.DataFrame()
    last_positions = pd.DataFrame()
    
    for symbol in symbols:
        ignored_symbols = []  # To store symbols not found in both NSE and BSE

        symbol_trans = (
        df_transactions[df_transactions['Symbol'] == symbol]
        .reset_index()
        .sort_values(['Transaction Date', 'index'],kind='mergesort')
        .groupby('Transaction Date', as_index=False)
        .last()
        .drop(columns='index')
        )

        # start_date = symbol_trans['Transaction Date'].min() - timedelta(days=1)
        start_date = datetime.today() - timedelta(days=10)
        end_date = datetime.today()

        price_df = get_best_price_history(symbol, start_date, end_date)
        if price_df is None or price_df.empty:
            print(f"ℹ️ Trying manual CSV fallback for {symbol}...")
            price_df = get_manual_price_history(symbol, start_date, end_date, manual_data_dir)

        if price_df is None or price_df.empty:
            print(f"⚠️ Skipping symbol {symbol} — no data from NSE, BSE or manual CSV.")
            ignored_symbols.append(symbol)
            continue

        date_range = pd.date_range(start=symbol_trans['Transaction Date'].min(), 
                                   end=end_date, freq='D')
        date_df = pd.DataFrame({'Transaction Date': date_range})
        share_changes = symbol_trans.set_index('Transaction Date')['Total Shares']
        shares_ffilled = share_changes.reindex(date_range, method='ffill').fillna(0)

        merged = date_df.copy()
        merged['Total Shares'] = shares_ffilled.values
        merged['Symbol'] = symbol

        merged = pd.merge(merged, price_df, on='Transaction Date', how='left')
        merged['Price'].ffill(inplace=True)
        merged['Total value'] = merged['Total Shares'] * merged['Price']

        final_df = pd.concat([final_df, merged], ignore_index=True)
        last_position = merged.iloc[-1].copy()
        last_positions = pd.concat([last_positions, last_position.to_frame().T], ignore_index=True)

    final_df = final_df[['Symbol', 'Transaction Date', 'Total Shares', 'Price', 'Total value']]
    last_positions = last_positions[['Symbol', 'Transaction Date', 'Price', 'Total Shares', 'Total value']]
    last_positions.columns = ['Symbol', 'As of Date', 'Last Price', 'Total Shares', 'Total Value']

    portfolio_value = final_df.groupby('Transaction Date')['Total value'].sum().reset_index()
    portfolio_value.columns = ['Transaction Date', 'Portfolio Value']

    final_df.to_csv('/Users/in22417145/PycharmProjects/portfolio/data/per_symbol_values.csv', index=False)
    portfolio_value.to_csv(output_csv_path, index=False)
    last_positions.to_csv('/Users/in22417145/PycharmProjects/portfolio/data/last_day_values.csv', index=False)

    print("✅ Per-symbol daily values saved.")
    print("✅ Aggregated portfolio values saved.")
    print("✅ Last positions report saved.")

    # Print ignored symbols if any
    if ignored_symbols:
        print("\n⚠️ The following symbols were ignored as they were not found in both NSE and BSE:")
        for s in ignored_symbols:
            print(f"  - {s}")
    else:
        print("\n✅ All symbols were successfully processed from either NSE or BSE.")

# Example usage
input_csv_path = '/Users/in22417145/PycharmProjects/portfolio/data/ind-stocks.csv'
output_csv_path = '/Users/in22417145/PycharmProjects/portfolio/data/ind-stocks-output.csv'
get_portfolio_values(input_csv_path, output_csv_path)
