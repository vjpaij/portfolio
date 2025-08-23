import pandas as pd
from datetime import datetime

def process_csv(input_csv, output_csv):
    # Read CSV
    df = pd.read_csv(input_csv)
    
    # Ensure date is in datetime format
    df['date'] = pd.to_datetime(df['date'])
    
    # Step 1: Aggregate amounts by date
    df = df.groupby('date', as_index=False)['amount'].sum()
    
    # Step 2: Compute cumulative sum
    df['cumulative_amount'] = df['amount'].cumsum()
    
    # Step 3: Create full date range from min to max
    full_range = pd.date_range(start=df['date'].min(), end=df['date'].max(), freq='D')
    
    # Step 4: Reindex dataframe to include all dates
    df = df.set_index('date').reindex(full_range)
    
    # Step 5: Forward fill cumulative amounts
    df['cumulative_amount'] = df['cumulative_amount'].ffill()
    
    # Step 6: Replace NaN amounts with 0 (since those dates had no transactions)
    df['amount'] = df['amount'].fillna(0)
    
    # Step 7: Reset index and rename date column
    df = df.reset_index().rename(columns={'index': 'date'})
    
    # Step 8: Save to new CSV
    df.to_csv(output_csv, index=False)
    print(f"Processed file saved as: {output_csv}")

# Example usage
process_csv("/Users/in22417145/PycharmProjects/portfolio/data/credit_card.csv", 
            "/Users/in22417145/PycharmProjects/portfolio/data/credit_output.csv")
