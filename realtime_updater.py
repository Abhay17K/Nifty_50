import time
import schedule
import data_fetcher
import database
from datetime import datetime
import pytz

# Constants
IST = pytz.timezone('Asia/Kolkata')

def is_market_open():
    """
    Check if NSE market is open (9:15 AM - 3:30 PM IST, Mon-Fri)
    """
    now_ist = datetime.now(IST)
    
    # Check if weekend (Saturday=5, Sunday=6)
    if now_ist.weekday() >= 5:
        return False
        
    current_time = now_ist.time()
    market_start = datetime.strptime("09:15", "%H:%M").time()
    market_end = datetime.strptime("15:30", "%H:%M").time()
    
    return market_start <= current_time <= market_end

def update_realtime_data():
    """
    Fetch latest data and update database for all timeframes
    """
    print(f"[{datetime.now(IST)}] Checking for updates...")
    
    # Even if market is closed, we might want to run this once to ensure we have latest data
    # But for strict real-time loop, we can check market status
    
    if not is_market_open():
        print("Market is closed.")
        # Optional: You can choose to skip update if market is closed
        # return 
        
    try:
        # Update 15m data
        try:
            print("Updating 15m data...")
            df_15m = data_fetcher.fetch_latest_data('15m')
            if not df_15m.empty:
                database.store_data(df_15m, '15m')
        except Exception as e:
            print(f"Error updating 15m: {e}")
            
        # Update 1h data
        try:
            print("Updating 1h data...")
            df_1h = data_fetcher.fetch_latest_data('1h')
            if not df_1h.empty:
                database.store_data(df_1h, '1h')
        except Exception as e:
            print(f"Error updating 1h: {e}")
            
        # Update Daily data (to get current day's candle)
        try:
            print("Updating Daily data...")
            df_1d = data_fetcher.fetch_nifty_data('1d', '5d')
            if not df_1d.empty:
                database.store_data(df_1d, '1d')
        except Exception as e:
            print(f"Error updating 1d: {e}")
            
        # Run signal and indicator processing
        try:
            import process_data
            process_data.process_hourly_signals()
            process_data.process_daily_signals()
        except Exception as e:
            print(f"Error processing signals/indicators: {e}")
            
        print("Update cycle completed.")
        
    except Exception as e:
        print(f"Error in update_realtime_data: {e}")

def start_scheduler():
    print("Starting optimized real-time data scheduler...")
    print("Updates will trigger every 5 minutes during market hours (starting 9:15 AM).")
    print("Press Ctrl+C to stop.")
    
    # Schedule updates every 5 minutes
    schedule.every(5).minutes.do(update_realtime_data)
    
    # Run once immediately if it's currently market hours
    if is_market_open():
        update_realtime_data()
    
    while True:
        try:
            schedule.run_pending()
            time.sleep(1)
        except KeyboardInterrupt:
            print("Scheduler stopped.")
            break
        except Exception as e:
            print(f"Scheduler error: {e}")
            time.sleep(5)

if __name__ == "__main__":
    start_scheduler()
