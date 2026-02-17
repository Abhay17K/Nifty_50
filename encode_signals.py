import sqlite3

def encode_columns():
    conn = sqlite3.connect('nifty50_data.db')
    cursor = conn.cursor()
    
    print("Encoding signal and trend columns in features_merged...")
    
    # 1. Add signal column if it doesn't exist
    cursor.execute("PRAGMA table_info(features_merged)")
    cols = [r[1] for r in cursor.fetchall()]
    
    if 'signal' not in cols:
        print("Adding 'signal' column...")
        cursor.execute("ALTER TABLE features_merged ADD COLUMN signal INTEGER")
    else:
        print("'signal' column already exists")
    
    # 2. Encode target -> signal
    # PUT = 0, SIDEWAYS = 1, CALL = 2
    print("Encoding target column to signal...")
    cursor.execute("""
        UPDATE features_merged
        SET signal = CASE
            WHEN target = 'PUT' THEN 0
            WHEN target = 'SIDEWAYS' THEN 1
            WHEN target = 'CALL' THEN 2
            ELSE NULL
        END
    """)
    
    # 3. Encode daily_trend_flag
    # bearish → -1, bullish → 1
    if 'daily_trend_flag' in cols:
        print("Encoding daily_trend_flag column...")
        cursor.execute("""
            UPDATE features_merged
            SET daily_trend_flag = CASE
                WHEN LOWER(daily_trend_flag) = 'bearish' THEN -1
                WHEN LOWER(daily_trend_flag) = 'bullish' THEN 1
                ELSE daily_trend_flag
            END
        """)
    else:
        print("Warning: daily_trend_flag column not found")
    
    # 4. Verify encodings
    print("\nVerifying encodings...")
    cursor.execute("SELECT signal, COUNT(*) FROM features_merged GROUP BY signal")
    signal_counts = cursor.fetchall()
    print("Signal distribution:")
    for val, count in signal_counts:
        label = {0: 'PUT', 1: 'SIDEWAYS', 2: 'CALL', None: 'NULL'}.get(val, val)
        print(f"  {label} ({val}): {count} rows")
    
    if 'daily_trend_flag' in cols:
        cursor.execute("SELECT daily_trend_flag, COUNT(*) FROM features_merged GROUP BY daily_trend_flag")
        trend_counts = cursor.fetchall()
        print("\nDaily trend distribution:")
        for val, count in trend_counts:
            label = {-1: 'bearish', 1: 'bullish', None: 'NULL'}.get(val, val)
            print(f"  {label} ({val}): {count} rows")
    
    conn.commit()
    conn.close()
    print("\nEncoding complete!")

if __name__ == "__main__":
    encode_columns()
