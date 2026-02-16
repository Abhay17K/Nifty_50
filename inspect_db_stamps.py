import sqlite3

def check_db():
    conn = sqlite3.connect('nifty50_data.db')
    c = conn.cursor()
    
    # Check for latest timestamps and their targets
    print("Latest 20 Hourly entries sorted by string:")
    c.execute("SELECT timestamp, target FROM nifty_1h ORDER BY timestamp DESC LIMIT 20")
    for row in c.fetchall():
        print(f"Timestamp: '{row[0]}', Target: '{row[1]}'")
        
    print("\nEntries for today (Feb 16) specifically:")
    c.execute("SELECT timestamp, target FROM nifty_1h WHERE timestamp LIKE '2026-02-16%'")
    for row in c.fetchall():
        print(f"Timestamp: '{row[0]}', Target: '{row[1]}'")
        
    conn.close()

if __name__ == "__main__":
    check_db()
