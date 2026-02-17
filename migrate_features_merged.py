import sqlite3
import os

DB_NAME = 'nifty50_data.db'

def migrate():
    if not os.path.exists(DB_NAME):
        print("Database not found.")
        return

    conn = sqlite3.connect(DB_NAME)
    c = conn.cursor()

    # 1. Ensure features_merged table exists and has identical base structure to nifty_1h
    # Plus the daily columns
    print("Fetching column info...")
    c.execute("PRAGMA table_info(nifty_1h)")
    h_cols = [row[1] for row in c.fetchall()]
    
    c.execute("PRAGMA table_info(nifty_1d)")
    d_info = c.fetchall()
    exclude = ['timestamp', 'open', 'high', 'low', 'close', 'date', 'time', 'volume', 'target']
    d_cols = [r[1] for r in d_info if r[1] not in exclude]

    # Create table if not exists with all columns
    # We'll just use the existing table structure if it's already there
    # But for the triggers, we need the exact column list
    
    c.execute("PRAGMA table_info(features_merged)")
    ml_cols = [row[1] for row in c.fetchall()]

    col_str = ", ".join(ml_cols)
    
    # Identify which columns are hourly vs daily
    hourly_cols = [c for c in ml_cols if c in h_cols]
    daily_cols = [c for c in ml_cols if c.startswith('daily_')]

    # 3. Create Trigger for automatic synchronization (INSERT)
    print("Creating triggers with daily features lookup...")
    
    # Trigger for INSERT
    c.execute("DROP TRIGGER IF EXISTS sync_nifty_1h_insert")
    
    # Build VALUES list with daily lookups
    val_items = []
    for col in ml_cols:
        if col == 'signal':
            # Encode signal based on target: PUT=0, SIDEWAYS=1, CALL=2
            val_items.append("CASE WHEN new.target = 'PUT' THEN 0 WHEN new.target = 'SIDEWAYS' THEN 1 WHEN new.target = 'CALL' THEN 2 ELSE NULL END")
        elif col in h_cols:
            val_items.append(f"new.{col}")
        elif col.startswith('daily_'):
            orig_daily_col = col.replace('daily_', '', 1)
            val_items.append(f"(SELECT {orig_daily_col} FROM nifty_1d WHERE date = new.date LIMIT 1)")
        else:
            val_items.append("NULL")

    c.execute(f"""
        CREATE TRIGGER sync_nifty_1h_insert
        AFTER INSERT ON nifty_1h
        BEGIN
            INSERT OR REPLACE INTO features_merged ({col_str})
            VALUES ({', '.join(val_items)});
        END;
    """)

    # Trigger for UPDATE
    c.execute("DROP TRIGGER IF EXISTS sync_nifty_1h_update")
    
    # Build SET list
    set_items = []
    for col in ml_cols:
        if col == 'timestamp': continue
        if col == 'signal':
            # Encode signal based on target: PUT=0, SIDEWAYS=1, CALL=2
            set_items.append("signal = CASE WHEN new.target = 'PUT' THEN 0 WHEN new.target = 'SIDEWAYS' THEN 1 WHEN new.target = 'CALL' THEN 2 ELSE NULL END")
        elif col in h_cols:
            set_items.append(f"{col} = new.{col}")
        elif col.startswith('daily_'):
            orig_daily_col = col.replace('daily_', '', 1)
            set_items.append(f"{col} = (SELECT {orig_daily_col} FROM nifty_1d WHERE date = new.date LIMIT 1)")

    c.execute(f"""
        CREATE TRIGGER sync_nifty_1h_update
        AFTER UPDATE ON nifty_1h
        BEGIN
            UPDATE features_merged 
            SET {', '.join(set_items)}
            WHERE timestamp = new.timestamp;
        END;
    """)

    conn.commit()
    conn.close()
    print("Triggers updated with high-timeframe daily feature lookups.")

if __name__ == "__main__":
    migrate()
