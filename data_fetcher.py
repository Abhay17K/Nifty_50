import yfinance as yf
import pandas as pd
import pytz
from datetime import datetime, timedelta, time

# Constants
SYMBOL = "^NSEI"
IST = pytz.timezone('Asia/Kolkata')

def get_ist_time():
    return datetime.now(IST)

def fetch_nifty_data(interval, period="max"):
    """
    Fetch Nifty 50 data from Yahoo Finance.
    interval: '15m', '1h', '1d', '1wk'
    period: 'max', '1y', '5y', etc.
    """
    print(f"Fetching {interval} data for period: {period}...")
    
    try:
        # yfinance parameters
        # valid intervals: 1m,2m,5m,15m,30m,60m,90m,1h,1d,5d,1wk,1mo,3mo
        yf_interval = interval
        if interval == '1wk':
            yf_interval = '1wk' 
        elif interval == '15m':
             # Intraday data is limited. yfinance allows 60d for 15m
             if period == 'max':
                 period = '60d' # Max allowed for 15m
        elif interval == '1h':
             # Intraday data limited. yfinance allows 730d for 1h
             if period == 'max':
                 period = '730d' # Max allowed for 1h
                 
        ticker = yf.Ticker(SYMBOL)
        df = ticker.history(period=period, interval=yf_interval)
        
        if df.empty:
            print(f"No data received for {interval}")
            return df
            
        # Clean up data
        # Ensure timezone is IST
        if df.index.tz is None:
            # If naive, assume it's already in exchange time (IST) and localize
            df.index = df.index.tz_localize(IST)
        else:
            df.index = df.index.tz_convert(IST)
            
        # Filter Market Hours for Intraday (9:15 - 15:30)
        if interval in ['15m', '1h']:
            # Create a mask for time range
            # Note: yfinance 1h candles might be aligned to start of hour or market open. 
            # Usually starts at 9:15 for the first candle.
            
            # Remove any pre-market or post-market data if present
            # However, for 1D and 1WK we keep as is.
            pass
            
        # Rename columns to lowercase
        df.columns = [c.lower() for c in df.columns]
        
        # Ensure we have required columns
        required_cols = ['open', 'high', 'low', 'close', 'volume']
        # If volume is missing (sometimes for indices), fill with 0
        if 'volume' not in df.columns:
            df['volume'] = 0
            
        return df[required_cols]

    except Exception as e:
        print(f"Error in fetch_nifty_data: {e}")
        return pd.DataFrame()

def fetch_latest_data(interval='15m'):
    """
    Fetch the latest data (e.g. last 1 day or 5 days) to update real-time
    """
    # for real-time, we just need the last few candles
    period = '5d' 
    return fetch_nifty_data(interval, period)

if __name__ == "__main__":
    # Test fetching
    print("Testing data fetch...")
    df_15m = fetch_nifty_data('15m', '5d')
    print(f"15m data: {len(df_15m)} rows")
    if not df_15m.empty:
        print(df_15m.tail())
        
    df_daily = fetch_nifty_data('1d', '1y')
    print(f"Daily data: {len(df_daily)} rows")
    if not df_daily.empty:
        print(df_daily.tail())
