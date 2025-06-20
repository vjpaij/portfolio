import pandas as pd

# Step 1: Load the spreadsheet
df = pd.read_csv("/Users/in22417145/PycharmProjects/portfolio/data/sbi.csv")  # or pd.read_csv("your_file.csv") if it's a CSV
df['Transaction Date'] = pd.to_datetime(df['Transaction Date'])

"""
For dates in ascending order, you can use the following code 
"""

# Step 2: Add an index to preserve original row order
df['OriginalOrder'] = df.index

# Step 3: Sort by Date but keep original order for tie-breaking
df_sorted = df.sort_values(by=['Transaction Date', 'OriginalOrder'])

# Step 4: Drop duplicates, keeping the LAST occurrence (i.e., the one with highest OriginalOrder per date)
# For Date in ascending order
df_deduped = df_sorted.drop_duplicates(subset='Transaction Date', keep='last')

# Step 5: Set Date as index
df_deduped.set_index('Transaction Date', inplace=True)

# Step 6: Create complete date range
full_date_range = pd.date_range(start=df_deduped.index.min(), end=df_deduped.index.max(), freq='D')

# Step 7: Reindex and forward-fill
df_full = df_deduped.reindex(full_date_range).ffill()

# Step 8: Clean up and export
df_full.reset_index(inplace=True)
df_full.columns = ['Transaction Date', 'Balance', 'OriginalOrder']
df_final = df_full[['Transaction Date', 'Balance']]  # remove helper column

# Step 8: Save the new cleaned-up file
df_full.to_csv("/Users/in22417145/PycharmProjects/portfolio/data/cleaned_output.csv", index=False)

"""
For dates in descending order, you can use the following code 
"""

# # For Date in descending order
# # Step 2: Convert 'Date' to datetime

# # Step 3: Preserve original order (descending), drop duplicate dates keeping the first one seen
# df = df.drop_duplicates(subset='Transaction Date', keep='first')

# # Step 4: Sort by date ascending to fill in missing dates properly
# df = df.sort_values('Transaction Date')

# # Step 5: Set Date as index
# df = df.set_index('Transaction Date')

# # Step 6: Create full date range and reindex
# full_range = pd.date_range(start=df.index.min(), end=df.index.max(), freq='D')
# df = df.reindex(full_range)

# # Step 7: Forward-fill missing Closing Balance values
# df['Closing Balance'] = df['Closing Balance'].ffill()

# # Step 8: Rename index back to 'Date'
# df.index.name = 'Transaction Date'

# # Step 9: Save result to a new file
# df.to_csv("/Users/in22417145/PycharmProjects/portfolio/data/output_filled.csv")
