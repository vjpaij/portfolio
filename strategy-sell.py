import pandas as pd
import yfinance as yf
import numpy as np
import datetime
import time
from concurrent.futures import ThreadPoolExecutor, as_completed

# ------------------------------
# CONFIG
# ------------------------------
X_PERCENT = 25                        # Configurable % above EMA crossover
ALERT_THRESHOLD = 6.5                 # % drop from high since crossover
INPUT_CSV = "data/ind-stocks.csv"
OUTPUT_CSV = "data/strategy-sell-booking.csv"
MAX_THREADS = 20
RETRY_COUNT = 3
SLEEP_BETWEEN_BATCH = 0.2
BELOW_EMA50 = 6.5 / 100           
BELOW_RSI9 = 29                    
# ------------------------------


# ------------------------------
# SUPPORT FUNCTIONS
# ------------------------------
def safe_history(ticker, period="1y"):
    """History fetch with retries."""
    for attempt in range(RETRY_COUNT):
        try:
            data = yf.Ticker(ticker).history(period=period)
            if not data.empty:
                return data
        except Exception:
            pass
        time.sleep(1)
    return pd.DataFrame()


def resolve_yahoo_ticker(symbol):
    """Try .NS first, then .BO."""
    for suffix in [".NS", ".BO"]:
        ticker = symbol + suffix
        data = safe_history(ticker, "5d")
        if not data.empty:
            return ticker
    return None


def get_latest_transaction(group):
    """Pick ONLY the latest transaction of the symbol.

    If latest row has Total Shares == 0 → ignore symbol completely.
    """
    latest = group.sort_values("Transaction Date").iloc[-1]

    if latest["Total Shares"] <= 0:
        return None

    return latest


def find_latest_ema_crossover(df):
    """Find latest Close > EMA50 crossover."""
    df["EMA50"] = df["Close"].ewm(span=50, adjust=False).mean()
    df["prev_close"] = df["Close"].shift(1)
    df["prev_ema"] = df["EMA50"].shift(1)

    cross = df[
        (df["prev_close"] <= df["prev_ema"]) &
        (df["Close"] > df["EMA50"])
    ]

    if cross.empty:
        return None

    return cross.iloc[-1]


def compute_rsi(series, period=9):
    """Compute RSI using Pandas, correctly aligned with index."""
    delta = series.diff()

    gain = delta.where(delta > 0, 0)
    loss = -delta.where(delta < 0, 0)

    avg_gain = gain.rolling(period, min_periods=period).mean()
    avg_loss = loss.rolling(period, min_periods=period).mean()

    rs = avg_gain / avg_loss
    rsi = 100 - (100 / (1 + rs))

    return rsi


# ------------------------------
# MAIN PER-SYMBOL PROCESSING
# ------------------------------
def process_symbol(symbol, latest_txn):
    txn_date = latest_txn["Transaction Date"].date()
    total_shares = latest_txn["Total Shares"]

    ticker = resolve_yahoo_ticker(symbol)
    if not ticker:
        return {"symbol": symbol, "reason": "No Yahoo ticker found"}

    data = safe_history(ticker, "1y")
    if data.empty:
        return {"symbol": symbol, "yahoo_symbol": ticker, "reason": "No price history"}

    # -----------------------------
    # COMPUTE EMA50 & RSI(9)
    # -----------------------------
    data["EMA50"] = data["Close"].ewm(span=50, adjust=False).mean()
    data["RSI9"] = compute_rsi(data["Close"], 9)

    # Latest values
    current_close = data["Close"].iloc[-1]
    current_ema50 = data["EMA50"].iloc[-1]
    current_rsi9 = data["RSI9"].iloc[-1]

    # -----------------------------
    # SELL CONDITIONS
    # -----------------------------
    cond1 = current_close < current_ema50 * (1 - BELOW_EMA50)  
    cond2 = current_rsi9 < BELOW_RSI9

    sell = "YES" if (cond1 and cond2) else "NO"

    # -----------------------------
    # EMA CROSSOVER LOGIC
    # -----------------------------
    crossover = find_latest_ema_crossover(data)
    if crossover is None:
        return {
            "symbol": symbol,
            #"yahoo_symbol": ticker,
            "reason": "No EMA crossover",
            "sell": sell,
            "current_price": current_close,
            "ema50": current_ema50,
            "rsi9": current_rsi9
        }

    crossover_date = crossover.name
    crossover_close = crossover["Close"]
    crossover_ema = crossover["EMA50"]

    # -----------------------------

    # % FROM CROSSOVER 50EMA
    # -----------------------------
    pct_from_crossover_ema = (
        (current_close - crossover_ema) / crossover_ema * 100
    )

    # -----------------------------------------
    # % DROP FROM HIGH SINCE CROSSOVER (ALERT)
    # -----------------------------------------
    after_cross = data.loc[crossover_date:]
    high_since_cross = after_cross["Close"].max()
    pct_below_high = (high_since_cross - current_close) / high_since_cross * 100
    alert = "YES" if pct_below_high > ALERT_THRESHOLD else "NO"

    # -----------------------------------------
    # X% ABOVE CROSSOVER CONDITION
    # -----------------------------------------
    required_price = crossover_close * (1 + X_PERCENT / 100)
    meets = current_close > required_price

    return {
        "symbol": symbol,
        #"yahoo_symbol": ticker,
        "latest_transaction_date": txn_date,
        "total_shares": total_shares,

        # Crossover details
        "crossover_date": crossover_date.date(),
        "crossover_close": round(crossover_close, 2),
        "crossover_ema": round(crossover_ema, 2),

        # Price details
        "current_price": round(current_close, 2),
        "pct_from_crossover_ema": round(pct_from_crossover_ema, 2),
        "ema50": round(current_ema50, 2),
        "rsi9": round(current_rsi9, 2),

        # High since crossover
        "high_since_crossover": round(high_since_cross, 2),
        "pct_below_high": round(pct_below_high, 2),

        # Alert & Sell conditions
        "alert": alert,
        "sell": sell,

        # X% rule
        "required_price_for_Xpct": round(required_price, 2),
        "Xpct_condition_met": meets
    }


# ------------------------------
# PROGRAM ENTRY
# ------------------------------
df = pd.read_csv(INPUT_CSV)
df["Transaction Date"] = pd.to_datetime(df["Transaction Date"])

groups = df.groupby("Symbol")

tasks = []
results = []

with ThreadPoolExecutor(max_workers=MAX_THREADS) as executor:
    for symbol, group in groups:
        latest = get_latest_transaction(group)

        if latest is None:
            continue  # skip entire symbol

        tasks.append(executor.submit(process_symbol, symbol, latest))
        time.sleep(SLEEP_BETWEEN_BATCH)

    for task in as_completed(tasks):
        results.append(task.result())

pd.DataFrame(results).to_csv(OUTPUT_CSV, index=False)
print("Done. Output written to:", OUTPUT_CSV)
