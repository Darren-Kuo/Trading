import pandas as pd
import yfinance as yf


def fetch_stock_data(symbol: str, start: str, end: str) -> pd.DataFrame:
    """Fetch historical stock data from Yahoo Finance."""
    data = yf.download(symbol + ".TW", start=start, end=end)
    return data


def ma_strategy(data: pd.DataFrame, short_window: int = 5, long_window: int = 20) -> pd.DataFrame:
    """Calculate moving average crossover signals."""
    df = data.copy()
    df["MA_short"] = df["Close"].rolling(window=short_window).mean()
    df["MA_long"] = df["Close"].rolling(window=long_window).mean()
    df["Signal"] = 0
    df.loc[df["MA_short"] > df["MA_long"], "Signal"] = 1
    df.loc[df["MA_short"] < df["MA_long"], "Signal"] = -1
    df["Position"] = df["Signal"].diff()
    return df


if __name__ == "__main__":
    symbol = "2330"  # Taiwan Semiconductor Manufacturing Company
    start_date = "2020-01-01"
    end_date = "2023-12-31"
    stock_data = fetch_stock_data(symbol, start=start_date, end=end_date)
    result = ma_strategy(stock_data)
    print(result[["Close", "MA_short", "MA_long", "Position"]].dropna().tail())
