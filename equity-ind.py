'''
For Indian Stocks
'''

import os
import pandas as pd
import yfinance as yf
from datetime import datetime, timedelta
import warnings
warnings.filterwarnings('ignore')

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
        return None


def download_variant_price_history(symbols, start_date, end_date):
    """Download all NSE and BSE variant price history in one batch."""
    variants = [f"{symbol}.NS" for symbol in symbols] + [f"{symbol}.BO" for symbol in symbols]
    try:
        price_data = yf.download(
            tickers=variants,
            start=start_date,
            end=end_date,
            auto_adjust=False,
            group_by='ticker',
            threads=True,
            progress=False
        )
    except Exception:
        return None

    if price_data is None or price_data.empty:
        return None

    return price_data


def get_best_price_history_from_download(symbol_base, price_data):
    """Extract the best available price series for a symbol from downloaded data."""
    variants = [f"{symbol_base}.NS", f"{symbol_base}.BO"]
    series_list = []

    if isinstance(price_data.columns, pd.MultiIndex):
        for variant in variants:
            if (variant, 'Close') in price_data.columns:
                series = price_data[(variant, 'Close')].rename(variant)
                series_list.append(series)
    else:
        for variant in variants:
            if variant in price_data.columns:
                series = price_data[variant].rename(variant)
                series_list.append(series)

    if not series_list:
        return None

    merged = pd.concat(series_list, axis=1)
    merged['Price'] = merged.max(axis=1, skipna=True)
    merged['Price'] = merged['Price'].ffill().fillna(0)

    final = merged[['Price']].reset_index()
    final.columns = ['Transaction Date', 'Price']
    final['Transaction Date'] = pd.to_datetime(final['Transaction Date'])

    return final

def get_portfolio_values(input_csv_path, output_csv_path, days_filter=None):
    """
    Calculate portfolio values for all symbols.
    
    Parameters:
    - input_csv_path: Path to transactions CSV
    - output_csv_path: Path to save aggregated portfolio values
    - days_filter: If set, output only includes last N days (but prices fetched from inception)
    """
    manual_data_dir = './data'  # Local data directory
    
    # Load and preprocess transactions
    df_transactions = pd.read_csv(input_csv_path)
    df_transactions['Symbol'] = df_transactions['Symbol'].astype(str)
    df_transactions['Transaction Date'] = pd.to_datetime(df_transactions['Transaction Date'], dayfirst=True)
    
    # Determine date range for price fetching
    min_trans_date = df_transactions['Transaction Date'].min()
    end_date = datetime.today()
    
    # KEY FIX: Fetch prices from first transaction date, not from arbitrary days_filter
    price_start_date = min_trans_date - timedelta(days=1)
    
    symbols = df_transactions['Symbol'].unique()
    final_df = pd.DataFrame()
    last_positions = pd.DataFrame()
    ignored_symbols = []
    
    print(f"Downloading price history for {len(symbols)} symbols from {price_start_date.date()} to {end_date.date()}...")
    all_price_data = download_variant_price_history(symbols, price_start_date, end_date)
    
    for idx, symbol in enumerate(symbols, 1):
        print(f"[{idx}/{len(symbols)}] Processing {symbol}...", end='\r')
        
        symbol_trans = (
            df_transactions[df_transactions['Symbol'] == symbol]
            .reset_index()
            .sort_values(['Transaction Date', 'index'], kind='mergesort')
            .groupby('Transaction Date', as_index=False)
            .last()
            .drop(columns='index')
        )

        price_df = None
        if all_price_data is not None:
            price_df = get_best_price_history_from_download(symbol, all_price_data)

        if price_df is None or price_df.empty:
            price_df = get_manual_price_history(symbol, price_start_date, end_date, manual_data_dir)

        if price_df is None or price_df.empty:
            ignored_symbols.append(symbol)
            continue

        # Create full date range from first transaction to today
        date_range = pd.date_range(start=symbol_trans['Transaction Date'].min(), 
                                   end=end_date, freq='D')
        date_df = pd.DataFrame({'Transaction Date': date_range})
        
        # Forward fill shares
        share_changes = symbol_trans.set_index('Transaction Date')['Total Shares']
        shares_ffilled = share_changes.reindex(date_range, method='ffill').fillna(0)

        merged = date_df.copy()
        merged['Total Shares'] = shares_ffilled.values
        merged['Symbol'] = symbol

        # Merge with prices
        merged = pd.merge(merged, price_df, on='Transaction Date', how='left')
        merged['Price'].fillna(method='ffill', inplace=True)
        merged['Price'].fillna(0, inplace=True)
        merged['Total value'] = merged['Total Shares'] * merged['Price']

        # Apply days filter if specified (filter output, not data fetching)
        if days_filter is not None:
            filter_date = datetime.today() - timedelta(days=days_filter)
            merged = merged[merged['Transaction Date'] >= filter_date]

        if not merged.empty:
            final_df = pd.concat([final_df, merged], ignore_index=True)
            last_position = merged.iloc[-1].copy()
            last_positions = pd.concat([last_positions, last_position.to_frame().T], ignore_index=True)

    print(" " * 80)  # Clear progress line
    
    if final_df.empty:
        print("❌ No data to process!")
        return

    # Prepare output dataframes
    final_df = final_df[['Symbol', 'Transaction Date', 'Total Shares', 'Price', 'Total value']]
    last_positions = last_positions[['Symbol', 'Transaction Date', 'Price', 'Total Shares', 'Total value']]
    last_positions.columns = ['Symbol', 'As of Date', 'Last Price', 'Total Shares', 'Total Value']

    # Aggregate portfolio values
    portfolio_value = final_df.groupby('Transaction Date')['Total value'].sum().reset_index()
    portfolio_value.columns = ['Transaction Date', 'Portfolio Value']

    # Save outputs
    final_df.to_csv('data/per_symbol_values.csv', index=False)
    portfolio_value.to_csv(output_csv_path, index=False)
    last_positions.to_csv('data/last_day_values.csv', index=False)

    print("✅ Per-symbol daily values saved.")
    print("✅ Aggregated portfolio values saved.")
    print("✅ Last positions report saved.")

    # Print ignored symbols if any
    if ignored_symbols:
        print(f"\n⚠️  {len(ignored_symbols)} symbol(s) ignored (no data from NSE, BSE or manual CSV):")
        for s in ignored_symbols[:10]:  # Show first 10
            print(f"  - {s}")
        if len(ignored_symbols) > 10:
            print(f"  ... and {len(ignored_symbols) - 10} more")
    else:
        print("\n✅ All symbols were successfully processed from either NSE or BSE.")

# Example usage
if __name__ == "__main__":
    input_csv_path = 'data/ind-stocks.csv'
    output_csv_path = 'data/ind-stocks-output.csv'
    
    # Run with optional days_filter (e.g., 15 for last 15 days)
    # If days_filter is None, all data is included
    get_portfolio_values(input_csv_path, output_csv_path, days_filter=15)
    
    # Example: To get only last 15 days in output:
    # get_portfolio_values(input_csv_path, output_csv_path, days_filter=15)
