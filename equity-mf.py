'''
For Mutual Funds
'''


import pandas as pd
import requests
from datetime import datetime, timedelta
from io import StringIO

AMFI_NAV_HISTORY_URL = 'https://portal.amfiindia.com/DownloadNAVHistoryReport_Po.aspx'

def get_amfi_nav_history(start_date, end_date, scheme_codes):
    params = {
        'tp': 1,
        'frmdt': start_date.strftime('%d-%b-%Y'),
        'todt': end_date.strftime('%d-%b-%Y'),
    }
    response = requests.get(
        AMFI_NAV_HISTORY_URL,
        params=params,
        headers={'User-Agent': 'Mozilla/5.0'},
        timeout=90,
    )
    response.raise_for_status()

    nav = pd.read_csv(
        StringIO(response.text),
        sep=';',
        skipinitialspace=True,
        dtype=str,
    )
    nav.columns = nav.columns.str.strip()
    nav['Scheme Code'] = nav['Scheme Code'].astype(str).str.strip()
    nav['Transaction Date'] = pd.to_datetime(
        nav['Date'].astype(str).str.strip(),
        dayfirst=True,
        errors='coerce',
    )
    nav['Price'] = pd.to_numeric(nav['Net Asset Value'], errors='coerce')
    nav = nav[
        nav['Scheme Code'].isin(scheme_codes)
        & nav['Transaction Date'].notna()
        & nav['Price'].notna()
    ]
    return nav[['Scheme Code', 'Transaction Date', 'Price']]

def get_portfolio_values(input_csv_path, output_csv_path):
    # Read the input CSV file
    df_transactions = pd.read_csv(input_csv_path)
    
    # Ensure Symbol column is treated as string
    df_transactions['Symbol'] = df_transactions['Symbol'].astype(str)
    
    # Convert Transaction Date to datetime
    df_transactions['Transaction Date'] = pd.to_datetime(df_transactions['Transaction Date'], dayfirst=True)
    
    # Get unique AMFI scheme codes
    symbols = df_transactions['Symbol'].unique()
    start_date = datetime.today() - timedelta(days=15)
    end_date = datetime.today()
    nav_history = get_amfi_nav_history(start_date, end_date, symbols)
    
    # Initialize DataFrames for outputs
    final_df = pd.DataFrame()
    last_positions = pd.DataFrame()
    
    for symbol in symbols:
        # Get transactions for this symbol and keep only last transaction per date
        symbol_trans = (df_transactions[df_transactions['Symbol'] == symbol]
                       .sort_values('Transaction Date')
                       .drop_duplicates('Transaction Date', keep='last')
                       .copy())
        
        try:
            # Select historical NAVs for this AMFI scheme code.
            hist = nav_history[nav_history['Scheme Code'] == symbol]
            if hist.empty:
                print(f"No AMFI NAV data found for {symbol}")
                continue
            
            # Create a date range from first transaction to today
            date_range = pd.date_range(start=symbol_trans['Transaction Date'].min(), 
                                     end=end_date, freq='D')
            date_df = pd.DataFrame({'Transaction Date': date_range})
            
            # Create a Series with all transaction dates and their share values
            share_changes = symbol_trans.set_index('Transaction Date')['Total Shares']
            
            # Reindex to all dates and forward fill
            shares_ffilled = share_changes.reindex(date_range, method='ffill').fillna(0)
            
            # Create the merged DataFrame
            merged = date_df.copy()
            merged['Total Shares'] = shares_ffilled.values
            merged['Symbol'] = symbol
            
            # Merge with historical prices
            merged = pd.merge(merged, hist, on='Transaction Date', how='left')
            
            # Forward fill prices for bank holidays
            merged['Price'] = merged['Price'].ffill()
            
            # Calculate total value
            merged['Total value'] = merged['Total Shares'] * merged['Price']
            
            # Append to final DataFrame
            final_df = pd.concat([final_df, merged], ignore_index=True)
            
            # Get last position for this symbol
            last_position = merged.iloc[-1].copy()
            last_positions = pd.concat([last_positions, last_position.to_frame().T], ignore_index=True)
            
        except Exception as e:
            print(f"Error processing {symbol}: {str(e)}")
            continue
    
    # Reorder columns for daily values
    final_df = final_df[['Symbol', 'Transaction Date', 'Total Shares', 'Price', 'Total value']]
    
    # Prepare last positions report
    last_positions = last_positions[['Symbol', 'Transaction Date', 'Price', 'Total Shares', 'Total value']]
    last_positions.columns = ['Symbol', 'As of Date', 'Last Price', 'Total Shares', 'Total Value']
    
    # Calculate aggregated portfolio value by date
    portfolio_value = final_df.groupby('Transaction Date')['Total value'].sum().reset_index()
    portfolio_value.columns = ['Transaction Date', 'Portfolio Value']
    
    # Save outputs to CSV files
    final_df.to_csv('data/per_symbol_values.csv', index=False)
    portfolio_value.to_csv(output_csv_path, index=False)
    last_positions.to_csv('data/last_day_values.csv', index=False)
    
    print(f"Per-symbol daily values saved to 'per_symbol_daily_values.csv'")
    print(f"Aggregated portfolio values saved to '{output_csv_path}'")
    print(f"Last positions report saved to 'last_positions_report.csv'")

# Example usage
input_csv_path = 'data/ind-mf.csv'
output_csv_path = 'data/ind-stocks-output.csv'
get_portfolio_values(input_csv_path, output_csv_path)


