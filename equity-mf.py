'''
For Mutual Funds
'''


import pandas as pd
import yfinance as yf
from datetime import datetime, timedelta

def get_portfolio_values(input_csv_path, output_csv_path):
    # Read the input CSV file
    df_transactions = pd.read_csv(input_csv_path)
    
    # Ensure Symbol column is treated as string
    df_transactions['Symbol'] = df_transactions['Symbol'].astype(str)
    
    # Convert Transaction Date to datetime
    df_transactions['Transaction Date'] = pd.to_datetime(df_transactions['Transaction Date'], dayfirst=True)
    
    # Get unique symbols
    symbols = df_transactions['Symbol'].unique()
    
    # Initialize DataFrames for outputs
    final_df = pd.DataFrame()
    last_positions = pd.DataFrame()
    
    for symbol in symbols:
        # Get transactions for this symbol and keep only last transaction per date
        symbol_trans = (df_transactions[df_transactions['Symbol'] == symbol]
                       .sort_values('Transaction Date')
                       .drop_duplicates('Transaction Date', keep='last')
                       .copy())
        
        # Get historical prices for this symbol
        start_date = symbol_trans['Transaction Date'].min() - timedelta(days=1)
        end_date = datetime.today()
        
        try:
            # Download historical data
            ticker = yf.Ticker(symbol)
            hist = ticker.history(start=start_date, end=end_date)
            
            if hist.empty:
                print(f"No data found for {symbol}")
                continue
            
            # Reset index and format date
            hist = hist.reset_index()
            hist['Date'] = pd.to_datetime(hist['Date']).dt.date
            hist['Transaction Date'] = pd.to_datetime(hist['Date'])
            
            # Keep only Date and Close price
            hist = hist[['Transaction Date', 'Close']]
            hist.rename(columns={'Close': 'Price'}, inplace=True)
            
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
            merged['Price'].ffill(inplace=True)
            
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
    final_df.to_csv('/Users/in22417145/PycharmProjects/portfolio/data/per_symbol_values.csv', index=False)
    portfolio_value.to_csv(output_csv_path, index=False)
    last_positions.to_csv('/Users/in22417145/PycharmProjects/portfolio/data/last_day_values.csv', index=False)
    
    print(f"Per-symbol daily values saved to 'per_symbol_daily_values.csv'")
    print(f"Aggregated portfolio values saved to '{output_csv_path}'")
    print(f"Last positions report saved to 'last_positions_report.csv'")

# Example usage
input_csv_path = '/Users/in22417145/PycharmProjects/portfolio/data/ind-mf.csv'
output_csv_path = '/Users/in22417145/PycharmProjects/portfolio/data/ind-stocks-output.csv'
get_portfolio_values(input_csv_path, output_csv_path)


