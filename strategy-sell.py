import pandas as pd
import yfinance as yf
import os
import tempfile
import time
from concurrent.futures import ThreadPoolExecutor, as_completed

# ------------------------------
# CONFIGURATION
# ------------------------------
INPUT_CSV = "data/ind-stocks.csv"
OUTPUT_CSV = "data/strategy-sell-booking.csv"
YF_CACHE_DIR = os.path.join(tempfile.gettempdir(), "portfolio-yfinance-cache")
YF_HISTORY_PERIOD = "5y"
MAX_THREADS = 12
RETRY_COUNT = 3
RETRY_DELAY_SECONDS = 1
SLEEP_BETWEEN_TASKS = 0.1

# Strategy thresholds (placeholders you can change)
CROSSOVER_GROWTH_PCT = 40.0        # Condition 1b: highest close after crossover must be >= 40% above crossover price
RED_CANDLE_DAYS = 4                # Condition 1c: consecutive red candles to check
BELOW_EMA21_PCT = 5.0              # Condition 1d: today's red candle close must be at least 5% below EMA21
BELOW_EMA50_PCT = 6.5              # Condition 2a: current close is more than 6.5% below EMA50
RSI_THRESHOLD = 26.0               # Condition 2b: current RSI10 is below 26
RSI_PERIOD = 10                    # RSI period for condition 2
EMA_SHORT = 21                     # Short EMA for crossover and Condition 1
EMA_LONG = 50                      # Long EMA for crossover and Condition 2

# ------------------------------
# HELPER FUNCTIONS
# ------------------------------

os.makedirs(YF_CACHE_DIR, exist_ok=True)
yf.cache.set_cache_location(YF_CACHE_DIR)


def safe_history(ticker, period="1y", interval="1d"):
    """Fetch Yahoo Finance history with retries."""
    for attempt in range(RETRY_COUNT):
        try:
            data = yf.Ticker(ticker).history(period=period, interval=interval)
            if not data.empty:
                return data
        except Exception:
            pass
        time.sleep(RETRY_DELAY_SECONDS)
    return pd.DataFrame()


def resolve_yahoo_ticker(symbol):
    """Fetch both NSE and BSE tickers, return the one with higher volume."""
    tickers = []
    for suffix in [".NS", ".BO"]:
        ticker = symbol + suffix
        data = safe_history(ticker, period="5d")
        if not data.empty:
            avg_volume = data["Volume"].mean()
            tickers.append((ticker, avg_volume))
    
    if not tickers:
        return None
    
    # Return ticker with highest average volume
    return max(tickers, key=lambda x: x[1])[0]


def compute_rsi(close_series, period=10):
    """Compute RSI using Wilder-style smoothing."""
    delta = close_series.diff()
    gain = delta.clip(lower=0)
    loss = -delta.clip(upper=0)

    avg_gain = gain.ewm(alpha=1 / period, adjust=False, min_periods=period).mean()
    avg_loss = loss.ewm(alpha=1 / period, adjust=False, min_periods=period).mean()
    rs = avg_gain / avg_loss
    rsi = 100 - (100 / (1 + rs))
    return rsi


def find_oldest_ema_crossover(df, start_date=None, short_span=EMA_SHORT, long_span=EMA_LONG):
    """Find the oldest EMA short crossover above EMA long on or after start_date."""
    short_col = f"EMA{short_span}"
    long_col = f"EMA{long_span}"

    if short_col not in df.columns:
        df[short_col] = df["Close"].ewm(span=short_span, adjust=False).mean()
    if long_col not in df.columns:
        df[long_col] = df["Close"].ewm(span=long_span, adjust=False).mean()

    prev_short = df[short_col].shift(1)
    prev_long = df[long_col].shift(1)

    cross = df[
        (prev_short <= prev_long) &
        (df[short_col] > df[long_col])
    ]

    if start_date is not None:
        start_date = pd.Timestamp(start_date).date()
        cross = cross[pd.Series(cross.index.date, index=cross.index) >= start_date]

    if cross.empty:
        return None
    return cross.iloc[0]


def get_latest_transaction(group):
    """Return the latest transaction row for a symbol, if active."""
    latest = group.sort_values("Transaction Date").iloc[-1]
    if latest["Total Shares"] <= 0:
        return None
    return latest


def get_current_open_dates(df):
    """Return each symbol's current open-position start date from running holdings."""
    df = df.copy()
    df["Total Shares"] = pd.to_numeric(df["Total Shares"], errors="coerce").fillna(0)
    df["OriginalOrder"] = range(len(df))

    open_dates = {}
    for symbol, group in df.sort_values(["Symbol", "Transaction Date", "OriginalOrder"]).groupby("Symbol"):
        open_date = None

        for _, row in group.iterrows():
            total_shares = int(row["Total Shares"])

            if total_shares > 0 and open_date is None:
                open_date = row["Transaction Date"]
            elif total_shares <= 0:
                open_date = None

        if open_date is not None:
            open_dates[symbol] = open_date

    return open_dates


def find_last_sell_indicator_date(history, crossover):
    """Return the latest date where either sell indicator was true."""
    cond1_dates = pd.DatetimeIndex([])

    if crossover is not None:
        crossover_close = float(crossover["Close"])
        after_cross = history.loc[crossover.name:].copy()
        after_cross["HighCloseSinceCross"] = after_cross["Close"].cummax()
        after_cross["HighGainSinceCross"] = (
            (after_cross["HighCloseSinceCross"] - crossover_close) / crossover_close * 100
        )

        recent_below_ema21 = (
            after_cross["Close"]
            .lt(after_cross["EMA21"])
            .rolling(RED_CANDLE_DAYS)
            .sum()
            .eq(RED_CANDLE_DAYS)
        )
        recent_red = (
            after_cross["Close"]
            .lt(after_cross["Open"])
            .rolling(RED_CANDLE_DAYS)
            .sum()
            .eq(RED_CANDLE_DAYS)
        )
        close_gap_ema21 = (after_cross["Close"] - after_cross["EMA21"]) / after_cross["EMA21"] * 100

        cond1 = (
            after_cross["HighGainSinceCross"].ge(CROSSOVER_GROWTH_PCT)
            & recent_below_ema21
            & recent_red
            & close_gap_ema21.le(-BELOW_EMA21_PCT)
        )
        cond1_dates = after_cross.index[cond1]

    ema50_gap_ok = history["Close"].lt(history["EMA50"] * (1 - BELOW_EMA50_PCT / 100))
    rsi_ok = history[f"RSI{RSI_PERIOD}"].lt(RSI_THRESHOLD)
    cond2_dates = history.index[ema50_gap_ok & rsi_ok]

    sell_dates = cond1_dates.union(cond2_dates)
    if sell_dates.empty:
        return None
    return sell_dates[-1].date()


# ------------------------------
# SYMBOL PROCESSING
# ------------------------------

def evaluate_conditions(symbol, latest_txn, open_date=None):
    """Evaluate both condition sets for a symbol and return detailed result."""
    output = {
        "symbol": symbol,
        "tran_date": latest_txn["Transaction Date"].date(),
        "open_date": open_date.date() if open_date is not None else None,
        "shares": int(latest_txn["Total Shares"]),
        "sell": "NO",
        "reason": None,
        "last_sell_date": None,
        "cond1": "NO",
        "cond2": "NO",
        "price": None,
        "ema21": None,
        "ema50": None,
        "rsi10": None,
        "x_date": None,
        "x_price": None,
        "hi_cls": None,
        "hi_gain": None,
        "hi_gain_ok": None,
        "l3_below21": None,
        "l3_red": None,
        "day3_ema21": None,
        "ema50_gap": None,
    }

    ticker = resolve_yahoo_ticker(symbol)
    if not ticker:
        output["reason"] = "No ticker"
        return output

    history = safe_history(ticker, period=YF_HISTORY_PERIOD)
    if history.empty:
        output["reason"] = "No history"
        return output

    history = history.copy()
    history["EMA21"] = history["Close"].ewm(span=EMA_SHORT, adjust=False).mean()
    history["EMA50"] = history["Close"].ewm(span=EMA_LONG, adjust=False).mean()
    history[f"RSI{RSI_PERIOD}"] = compute_rsi(history["Close"], RSI_PERIOD)

    latest_row = history.iloc[-1]
    current_close = float(latest_row["Close"])
    current_ema50 = float(latest_row["EMA50"])
    current_rsi = float(latest_row[f"RSI{RSI_PERIOD}"])

    output["price"] = round(float(latest_row["Close"]), 2)
    output["ema21"] = round(float(latest_row["EMA21"]), 2)
    output["ema50"] = round(float(latest_row["EMA50"]), 2)
    output["rsi10"] = round(float(latest_row[f"RSI{RSI_PERIOD}"]), 2)

    # Condition 1: the oldest EMA21-over-EMA50 crossover since the current
    # position opened, price is up from crossover, then the latest 3 daily
    # candles are red and below EMA21.
    crossover = find_oldest_ema_crossover(history, start_date=open_date)
    output["last_sell_date"] = find_last_sell_indicator_date(history, crossover)

    if crossover is not None:
        output["x_date"] = crossover.name.date()
        crossover_close = float(crossover["Close"])
        after_cross = history.loc[crossover.name:]
        high_close_after_cross = float(after_cross["Close"].max())

        output["x_price"] = round(crossover_close, 2)
        output["hi_cls"] = round(high_close_after_cross, 2)
        output["hi_gain"] = round(
            (high_close_after_cross - crossover_close) / crossover_close * 100,
            2,
        )
        output["hi_gain_ok"] = "YES" if output["hi_gain"] >= CROSSOVER_GROWTH_PCT else "NO"

        if output["hi_gain_ok"] == "YES" and len(history) >= RED_CANDLE_DAYS:
            # history only contains trading days (weekends/holidays omitted), so tail(3) is last 3 trading days
            recent = history.tail(RED_CANDLE_DAYS)
            below_ema21_all = (recent["Close"] < recent["EMA21"]).all()
            red_candles_all = (recent["Close"] < recent["Open"]).all()

            output["l3_below21"] = "YES" if below_ema21_all else "NO"
            output["l3_red"] = "YES" if red_candles_all else "NO"

            if below_ema21_all and red_candles_all:
                third_red = recent.iloc[-1]
                third_pct_ema21 = round(
                    (float(third_red["Close"]) - float(third_red["EMA21"])) / float(third_red["EMA21"]) * 100,
                    2,
                )

                output["day3_ema21"] = third_pct_ema21

                # The third candle in the 3-day red sequence is today's candle.
                if third_pct_ema21 <= -BELOW_EMA21_PCT:
                    output["cond1"] = "YES"
                    output["sell"] = "YES"
                    output["reason"] = "Condition 1"

    # Condition 2: current close weakness below EMA50 and RSI weakness
    if output["sell"] != "YES":
        if output["price"] is not None and output["ema50"] is not None:
            # Negative % means below EMA50
            output["ema50_gap"] = round(
                (current_close - current_ema50) / current_ema50 * 100,
                2,
            )

        cond2a = current_close < current_ema50 * (1 - BELOW_EMA50_PCT / 100)
        cond2b = current_rsi < RSI_THRESHOLD

        if cond2a and cond2b:
            output["cond2"] = "YES"
            output["sell"] = "YES"
            output["reason"] = "Condition 2"
        elif output["reason"] is None:
            output["reason"] = "No sell conditions met"

    return output


# ------------------------------
# MAIN PROGRAM
# ------------------------------

def run():
    df = pd.read_csv(INPUT_CSV)
    df["Transaction Date"] = pd.to_datetime(df["Transaction Date"], format="%d-%b-%y")
    open_dates = get_current_open_dates(df)

    grouped = df.groupby("Symbol")
    tasks = []
    results = []

    with ThreadPoolExecutor(max_workers=MAX_THREADS) as executor:
        for symbol, group in grouped:
            latest_txn = get_latest_transaction(group)
            if latest_txn is None:
                continue
            tasks.append(executor.submit(evaluate_conditions, symbol, latest_txn, open_dates.get(symbol)))
            time.sleep(SLEEP_BETWEEN_TASKS)

        for task in as_completed(tasks):
            results.append(task.result())

    result_df = pd.DataFrame(results)
    result_df = result_df.sort_values(["sell", "symbol"], ascending=[False, True])
    result_df.to_csv(OUTPUT_CSV, index=False)
    print(f"Done. Output written to: {OUTPUT_CSV}")


if __name__ == "__main__":
    run()
