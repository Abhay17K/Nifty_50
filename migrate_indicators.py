import sqlite3
import os

DB_NAME = 'nifty50_data.db'

def migrate_indicators():
    if not os.path.exists(DB_NAME):
        print("Database does not exist.")
        return

    conn = sqlite3.connect(DB_NAME)
    c = conn.cursor()
    
    # HOURLY INDICATORS
    hourly_cols = [
        ('rsi_14', 'REAL'), ('rsi_sma_14', 'REAL'), ('rsi_diff', 'REAL'), 
        ('rsi_slope', 'REAL'), ('rsi_dist_50', 'REAL'), ('rsi_zone', 'TEXT'),
        ('roc_7', 'REAL'), ('roc_9', 'REAL'), ('roc_21', 'REAL'),
        ('roc7_flag', 'INTEGER'), ('roc_accel', 'REAL'),
        ('hl_range', 'REAL'), ('range_pct', 'REAL'),
        ('ema_7', 'REAL'), ('ema_9', 'REAL'), ('ema_20', 'REAL'), 
        ('ema_50', 'REAL'), ('ema_100', 'REAL'), ('sma_25', 'REAL'),
        ('lsma_25', 'REAL'), ('close_gt_lsma', 'INTEGER'), ('close_lt_lsma', 'INTEGER'),
        ('close_pct_lsma', 'REAL'), ('lsma_diff', 'REAL'), ('close_pct_sma_25', 'REAL'), ('ema_alignment', 'TEXT'),
        ('bb_upper', 'REAL'), ('bb_lower', 'REAL'), ('bb_middle', 'REAL'),
        ('bb_width', 'REAL'), ('bb_squeeze', 'INTEGER'), ('bb_position', 'REAL'),
        ('bb_range', 'REAL'), ('bb_upper_slope', 'REAL'), ('bb_lower_slope', 'REAL'),
        ('atr_14', 'REAL'), ('atr_pct', 'REAL'),
        ('break_high_5', 'INTEGER'), ('break_low_5', 'INTEGER')
    ]

    # DAILY INDICATORS
    daily_cols = [
        ('rsi_14', 'REAL'), ('rsi_slope', 'REAL'),
        ('ema_20', 'REAL'), ('ema_20_slope', 'REAL'),
        ('trend_flag', 'TEXT')
    ]

    def add_columns(table, columns):
        print(f"Migrating table: {table}")
        for col_name, col_type in columns:
            try:
                c.execute(f"ALTER TABLE {table} ADD COLUMN {col_name} {col_type}")
                print(f"  Added {col_name}")
            except sqlite3.OperationalError:
                # Column already exists
                pass

    add_columns('nifty_1h', hourly_cols)
    add_columns('nifty_1d', daily_cols)
    
    conn.commit()
    conn.close()
    print("Migration complete.")

if __name__ == "__main__":
    migrate_indicators()
