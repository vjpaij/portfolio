
import pandas as pd
import yfinance as yf
from datetime import datetime, timedelta

"""
Strategy 1: Probablr Breakout
Daily Close > Daily Ema ( Daily Close , 50 ) 
1 day ago Close > 2 days ago Close 
1 day ago Close <= Daily Ema ( Daily Close , 50 ) 
Market Cap >= 1000 
Daily Volume > Daily Sma ( volume,20 ) * 5 
"""

INPUT_CSV = "data/strategy-breakout-input.csv"
OUTPUT_CSV = "data/strategy-breakout-output.csv"

"""
Strategy 2: Volume Shocker
Daily Volume > Daily Sma ( volume,20 ) * 5 
Market Cap >= 1000 
Daily "close - 1 candle ago close / 1 candle ago close * 100" >= 5 
Daily Ema ( Daily Close , 21 ) >= Daily Ema ( Daily Close , 50 ) 
"""

# INPUT_CSV = "data/strategy-volume-input.csv"
# OUTPUT_CSV = "data/strategy-volume-output.csv"


def detect_ticker(symbol):
    """Try NSE first, then BSE, return chosen ticker string."""
    nse = symbol + ".NS"
    bse = symbol + ".BO"

    if not yf.Ticker(nse).history(period="1d").empty:
        return nse
    if not yf.Ticker(bse).history(period="1d").empty:
        return bse
    return None


def fetch_prices_for_symbol(ticker, trade_dates):
    """
    Fetch today's price + price after each trade date.
    Only one download call for entire symbol.
    """

    # 1. Today's close
    today_close = yf.Ticker(ticker).history(period="1d")
    if today_close.empty:
        return None, {}

    today_price = float(today_close.iloc[-1]["Close"])

    # 2. Next-day price needs future days — find max date
    max_date = max(trade_dates)

    # download from oldest trade date until future buffer
    start = min(trade_dates) - timedelta(days=2)
    end   = max_date + timedelta(days=10)

    data = yf.download(ticker, start=start, end=end, progress=False)

    if data.empty:
        return None, {}

    next_day_price_map = {}

    # compute next-day open for each trade date
    for d in trade_dates:
        future_data = data[data.index > pd.Timestamp(d)]
        if future_data.empty:
            next_day_price_map[d] = None
        else:
            next_day_price_map[d] = float(future_data.iloc[0]["Open"])

    return today_price, next_day_price_map


def process_csv_fast(input_file, output_file):
    df = pd.read_csv(input_file)
    df["date"] = pd.to_datetime(df["date"])
    today_date = datetime.today().date()

    # remove today records
    df = df[df["date"].dt.date != today_date]

    # group by symbol
    symbols = df["symbol"].unique()

    ticker_map = {}
    today_price_cache = {}
    next_day_open_cache = {}

    # ---- DETECT TICKERS FIRST (NSE→BSE) ----
    for symbol in symbols:
        ticker = detect_ticker(symbol)
        if ticker:
            ticker_map[symbol] = ticker
        else:
            print(f"No ticker found for {symbol}, skipping...")
    
    # ---- FETCH PRICES PER SYMBOL (1 download per symbol) ----
    for symbol in symbols:
        if symbol not in ticker_map:
            continue
        
        ticker = ticker_map[symbol]
        trade_dates = list(df[df["symbol"] == symbol]["date"].dt.date.unique())

        today_price, next_day_map = fetch_prices_for_symbol(ticker, trade_dates)

        if today_price:
            today_price_cache[symbol] = today_price
            next_day_open_cache[symbol] = next_day_map


    # ---- APPLY CALCULATIONS ----
    def compute_row(row):
        symbol = row["symbol"]
        d = row["date"].date()

        if symbol not in next_day_open_cache: 
            return pd.Series([None, None, None, None])

        price = next_day_open_cache[symbol].get(d)
        today = today_price_cache.get(symbol)

        if not price or not today:
            return pd.Series([None, None, None, None])

        qty = round(10000 / price)
        pl = (today - price) * qty

        return pd.Series([price, today, qty, pl])

    df[["price", "today", "quantity", "pl"]] = df.apply(compute_row, axis=1)

    df.to_csv(output_file, index=False)
    print(f"Saved optimized output to: {output_file}")


# ---- RUN ----
process_csv_fast(INPUT_CSV, OUTPUT_CSV)
