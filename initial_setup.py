import data_fetcher
import database
import time

def initial_setup():
    print("Starting initial setup...")
    database.init_db()
    
    # 1. Fetch and store max available daily data
    print("\nFetching Daily Data (Max)...")
    df_daily = data_fetcher.fetch_nifty_data('1d', 'max')
    if not df_daily.empty:
        database.store_data(df_daily, '1d')
        
    # 2. Fetch and store max available weekly data
    print("\nFetching Weekly Data (Max)...")
    df_weekly = data_fetcher.fetch_nifty_data('1wk', 'max')
    if not df_weekly.empty:
        database.store_data(df_weekly, '1wk')
        
    # 3. Fetch and store max available hourly data (limited to 730 days)
    print("\nFetching Hourly Data (730d)...")
    df_hourly = data_fetcher.fetch_nifty_data('1h', 'max') # handled in fetcher
    if not df_hourly.empty:
        database.store_data(df_hourly, '1h')
        
    # 4. Fetch and store max available 15m data (limited to 60 days)
    print("\nFetching 15m Data (60d)...")
    df_15m = data_fetcher.fetch_nifty_data('15m', 'max') # handled in fetcher
    if not df_15m.empty:
        database.store_data(df_15m, '15m')
        
    print("\nInitial setup completed successfully!")

if __name__ == "__main__":
    initial_setup()
