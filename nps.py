import pandas as pd
import requests
from datetime import date

# --- Configuration ---
CSV_FILE_PATH = '/Users/in22417145/PycharmProjects/portfolio/data/nps.csv'
OUTPUT_CSV_FILE = '/Users/in22417145/PycharmProjects/portfolio/data/nps-total.csv'

SCHEME_TO_CODE = {
    "SBI PENSION FUND SCHEME E - TIER I Units": "SM001003",
    "ADITYA BIRLA SUNLIFE PENSION FUND SCHEME E - TIER I Units": "SM010001",
    "LIC PENSION FUND SCHEME E - TIER I Units": "SM003005",
    "NPS TRUST- A/C - UTI PENSION FUND SCHEME E - TIER I Units": "SM002003",
    "SBI PENSION FUND SCHEME C - TIER I Units": "SM001004",
    "ADITYA BIRLA SUNLIFE PENSION FUND SCHEME C - TIER I Units": "SM010002",
    "LIC PENSION FUND SCHEME C - TIER I Units": "SM003006",
    "NPS TRUST- A/C HDFC PENSION FUND MANAGEMENT LIMITED SCHEME C - TIER I Units": "SM008002",
    "SBI PENSION FUND SCHEME G - TIER I Units": "SM001005",
    "ADITYA BIRLA SUNLIFE PENSION FUND SCHEME G - TIER I Units": "SM010003",
    "LIC PENSION FUND SCHEME G - TIER I Units": "SM003007"
}

def get_historical_navs(scheme_code, scheme_name):
    api_url = f"https://npsnav.in/api/historical/{scheme_code}"
    print(f"Fetching full NAV history for '{scheme_name}'...")
    try:
        response = requests.get(api_url)
        response.raise_for_status()
        data = response.json().get('data', [])
        if not data:
            print(f"  -> No historical data found for {scheme_name}.")
            return None
        
        nav_df = pd.DataFrame(data)
        nav_df['date'] = pd.to_datetime(nav_df['date'], format='%d-%m-%Y')
        nav_df['nav'] = pd.to_numeric(nav_df['nav'])
        nav_df = nav_df.set_index('date')['nav'].sort_index()
        
        print(f"  -> Success! Fetched {len(nav_df)} NAV records.")
        return nav_df
        
    except requests.exceptions.RequestException as e:
        print(f"  -> API Error for {scheme_name}: {e}")
        return None
    except (ValueError, KeyError) as e:
        print(f"  -> Data Parsing Error for {scheme_name}: {e}")
        return None

if __name__ == "__main__":
    try:
        # 1. Load and prepare transaction data
        print(f"Step 1: Loading investment data from '{CSV_FILE_PATH}'...")
        transactions_df = pd.read_csv(CSV_FILE_PATH)
        transactions_df['Date'] = pd.to_datetime(transactions_df['Date'], dayfirst=True)
        transactions_df = transactions_df.sort_values(by='Date')
        print("  -> Data loaded and sorted by date.")

        # 2. Fetch all historical NAVs for schemes present in the transaction file
        print("\nStep 2: Fetching all required NAV histories...")
        unique_schemes_in_csv = transactions_df['Scheme'].unique()
        all_nav_data = {}
        for scheme_name in unique_schemes_in_csv:
            scheme_code = SCHEME_TO_CODE.get(scheme_name)
            if scheme_code:
                all_nav_data[scheme_name] = get_historical_navs(scheme_code, scheme_name)
            else:
                print(f"  -> Warning: Scheme code not found for '{scheme_name}'. It will be skipped.")

        # 3. Create pivot table of actual units (without cumulative sum)
        print("\nStep 3: Creating units table (without cumulative sum)...")
        daily_units = transactions_df.pivot_table(
            index='Date', columns='Scheme', values='Units', aggfunc='sum'
        ).fillna(0)
        print("  -> Units table created.")

        # 4. Create a complete date range and forward-fill the units
        start_date = daily_units.index.min()
        end_date = date.today()
        full_date_range = pd.date_range(start=start_date, end=end_date, freq='D')
        
        # Reindex to the full date range and forward-fill the units
        daily_units_held = daily_units.reindex(full_date_range).ffill().fillna(0)
        print("  -> Created full date range with forward-filled units.")

        # 5. Calculate portfolio value with detailed breakdown
        print("\nStep 4: Calculating portfolio value with detailed breakdown...")
        detailed_records = []
        
        for day in full_date_range:
            daily_total_value = 0
            day_record = {'Date': day}
            
            for scheme_name in daily_units_held.columns:
                units = daily_units_held.loc[day, scheme_name]
                if units > 0 and scheme_name in all_nav_data and all_nav_data[scheme_name] is not None:
                    nav_series = all_nav_data[scheme_name]
                    last_known_nav = nav_series.asof(day)
                    
                    if pd.notna(last_known_nav):
                        scheme_value = units * last_known_nav
                        daily_total_value += scheme_value
                        
                        day_record[f'{scheme_name}_Units'] = units
                        day_record[f'{scheme_name}_NAV'] = last_known_nav
                        day_record[f'{scheme_name}_Value'] = scheme_value
            
            day_record['Total_Value'] = daily_total_value
            detailed_records.append(day_record)
        
        print("  -> Detailed daily portfolio valuation complete.")

        # 6. Create and save the final report
        print(f"\nStep 5: Saving the corrected report to '{OUTPUT_CSV_FILE}'...")
        final_report = pd.DataFrame(detailed_records)
        final_report = final_report.set_index('Date')
        
        # Round all numeric values to 2 decimal places
        final_report = final_report.round(2)

        final_report.to_csv(OUTPUT_CSV_FILE)
        
        print("\n--- Process Complete! ---")
        print(f"Corrected report saved successfully to '{OUTPUT_CSV_FILE}'.")
        print("\nFinal Report Preview:")
        print(final_report.tail())
        print("-----------------------")

    except FileNotFoundError:
        print(f"\nError: The file '{CSV_FILE_PATH}' was not found.")
    except Exception as e:
        print(f"\nAn unexpected error occurred: {e}")