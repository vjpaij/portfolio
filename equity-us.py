'''
For US Stocks
'''


import pandas as pd
import yfinance as yf
from datetime import datetime, timedelta

def get_portfolio_values(input_csv_path, output_csv_path, days_filter=15):
    # Read the input CSV file
    df_transactions = pd.read_csv(input_csv_path)
    
    # Ensure Symbol column is treated as string
    df_transactions['Symbol'] = df_transactions['Symbol'].astype(str)
    
    # Convert Transaction Date to datetime
    df_transactions['Transaction Date'] = pd.to_datetime(df_transactions['Transaction Date'], dayfirst=True)

    # Determine overall date range for price fetching
    min_trans = df_transactions['Transaction Date'].min()
    price_start_date = min_trans - timedelta(days=1)
    end_date = datetime.today()

    # Get unique symbols
    symbols = df_transactions['Symbol'].unique()
    
    # Initialize DataFrames for outputs
    final_df = pd.DataFrame()
    last_positions = pd.DataFrame()

    # Download USD/INR exchange rates for full range
    inr_rate = yf.Ticker("INR=X").history(start=price_start_date, end=end_date)
    inr_rate = inr_rate.reset_index()
    # Normalize to date (drop timezone) to match transaction dates
    inr_rate['Transaction Date'] = pd.to_datetime(pd.to_datetime(inr_rate['Date']).dt.date)
    inr_rate = inr_rate[['Transaction Date', 'Close']]
    inr_rate.rename(columns={'Close': 'USDINR'}, inplace=True)

    for symbol in symbols:
        # Get transactions for this symbol and keep only last transaction per date
        symbol_trans = (df_transactions[df_transactions['Symbol'] == symbol]
                       .sort_values('Transaction Date')
                       .drop_duplicates('Transaction Date', keep='last')
                       .copy())
        
        # Download historical prices from first transaction date to today
        start_date = symbol_trans['Transaction Date'].min() - timedelta(days=1)
        try:
            ticker = yf.Ticker(symbol)
            hist = ticker.history(start=start_date, end=end_date)
            if hist.empty:
                # fallback to next symbol
                continue
            hist = hist.reset_index()
            # Normalize to date (drop timezone) to match transaction dates
            hist['Transaction Date'] = pd.to_datetime(pd.to_datetime(hist['Date']).dt.date)
            hist = hist[['Transaction Date', 'Close']]
            hist.rename(columns={'Close': 'Price'}, inplace=True)

            # Create a date range from first transaction to today
            date_range = pd.date_range(start=symbol_trans['Transaction Date'].min(), 
                                     end=end_date, freq='D')
            date_df = pd.DataFrame({'Transaction Date': date_range})

            # Create a Series with all transaction dates and their share values
            share_changes = symbol_trans.set_index('Transaction Date')['Total Shares']
            shares_ffilled = share_changes.reindex(date_range, method='ffill').fillna(0)

            # Create the merged DataFrame
            merged = date_df.copy()
            merged['Total Shares'] = shares_ffilled.values
            merged['Symbol'] = symbol

            # Merge with historical prices and forward fill
            merged = pd.merge(merged, hist, on='Transaction Date', how='left')
            merged['Price'].fillna(method='ffill', inplace=True)
            merged['Price'].fillna(0, inplace=True)

            # Calculate total value in USD
            merged['Total value (USD)'] = merged['Total Shares'] * merged['Price']

            # Merge with INR exchange rates and forward fill
            merged = pd.merge(merged, inr_rate, on='Transaction Date', how='left')
            merged['USDINR'].fillna(method='ffill', inplace=True)
            merged['USDINR'].fillna(method='bfill', inplace=True)

            # Calculate total value in INR
            merged['Total value (INR)'] = merged['Total value (USD)'] * merged['USDINR']

            # Apply days_filter to the output only
            if days_filter is not None:
                filter_date = datetime.today() - timedelta(days=days_filter)
                merged = merged[merged['Transaction Date'] >= filter_date]

            if not merged.empty:
                final_df = pd.concat([final_df, merged], ignore_index=True)
                last_position = merged.iloc[-1].copy()
                last_positions = pd.concat([last_positions, last_position.to_frame().T], ignore_index=True)

        except Exception as e:
            print(f"Error processing {symbol}: {str(e)}")
            continue

    # Reorder columns for daily values
    if not final_df.empty:
        final_df = final_df[['Symbol', 'Transaction Date', 'Total Shares', 'Price', 
                            'USDINR', 'Total value (USD)', 'Total value (INR)']]

    # Prepare last positions report
    if not last_positions.empty:
        last_positions = last_positions[['Symbol', 'Transaction Date', 'Price', 
                                       'USDINR', 'Total Shares', 'Total value (USD)', 
                                       'Total value (INR)']]
        last_positions.columns = ['Symbol', 'As of Date', 'Last Price (USD)', 
                                 'USD/INR Rate', 'Total Shares', 'Total Value (USD)', 
                                 'Total Value (INR)']

    # Calculate aggregated portfolio value by date (in INR)
    if not final_df.empty:
        portfolio_value = final_df.groupby('Transaction Date')['Total value (INR)'].sum().reset_index()
        portfolio_value.columns = ['Transaction Date', 'Portfolio Value (INR)']
    else:
        portfolio_value = pd.DataFrame(columns=['Transaction Date', 'Portfolio Value (INR)'])

    # Save outputs to CSV files
    final_df.to_csv('data/per_symbol_values.csv', index=False)
    portfolio_value.to_csv(output_csv_path, index=False)
    last_positions.to_csv('data/last_day_values.csv', index=False)

    print(f"Per-symbol daily values saved to 'data/per_symbol_values.csv'")
    print(f"Aggregated portfolio values saved to '{output_csv_path}'")
    print(f"Last positions report saved to 'data/last_day_values.csv'")

# Example usage
input_csv_path = 'data/us-stocks.csv'
output_csv_path = 'data/ind-stocks-output.csv'
get_portfolio_values(input_csv_path, output_csv_path)