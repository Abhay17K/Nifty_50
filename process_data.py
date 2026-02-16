import indicators
import database
import pandas as pd

def process_hourly_signals():
    print("Fetching hourly data from database...")
    df = database.get_data('1h', limit=100000)
    
    if df.empty:
        print("No hourly data found.")
        return

    print(f"Calculating indicators and signals for {len(df)} rows...")
    df = df.sort_index()

    # 1. Technical Indicators
    df = indicators.calculate_hourly_indicators(df)

    # 2. Shift close by 3 rows to create future_close (T+3 logic)
    df['future_close'] = df['close'].shift(-3)
    df['future_return'] = (df['future_close'] - df['close']) / df['close']

    # 3. Target columns
    def categorize_target(ret):
        if pd.isna(ret): return None
        if ret > 0.004: return 'CALL'
        elif ret < -0.004: return 'PUT'
        else: return 'SIDEWAYS'

    df['target'] = df['future_return'].apply(categorize_target)

    print("Storing processed hourly data...")
    database.store_data(df, '1h')
    
    # Save CSV reference
    df.dropna(subset=['target']).to_csv('nifty50_hourly_targets.csv')

def process_daily_signals():
    print("Fetching daily data from database...")
    df = database.get_data('1d', limit=100000)
    
    if df.empty:
        print("No daily data found.")
        return

    print("Calculating daily indicators...")
    df = df.sort_index()
    df = indicators.calculate_daily_indicators(df)

    print("Storing processed daily data...")
    database.store_data(df, '1d')

if __name__ == "__main__":
    process_hourly_signals()
    process_daily_signals()
