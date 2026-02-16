import sqlite3
import os

DB_NAME = 'nifty50_data.db'

def migrate():
    if not os.path.exists(DB_NAME):
        print("Database does not exist. Run init_db first.")
        return

    conn = sqlite3.connect(DB_NAME)
    c = conn.cursor()
    
    timeframes = ['15m', '1h', '1d', '1wk']
    
    for tf in timeframes:
        table_name = f'nifty_{tf}'
        try:
            print(f"Adding 'target' column to {table_name}...")
            c.execute(f"ALTER TABLE {table_name} ADD COLUMN target TEXT")
        except sqlite3.OperationalError:
            print(f"Column 'target' already exists in {table_name}.")
            
    conn.commit()
    conn.close()
    print("Migration complete.")

if __name__ == "__main__":
    migrate()
