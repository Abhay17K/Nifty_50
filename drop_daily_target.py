import sqlite3
import migrate_features_merged

def drop_daily_target():
    conn = sqlite3.connect('nifty50_data.db')
    cursor = conn.cursor()
    
    table = 'features_merged'
    col = 'daily_target'
    
    print(f"Checking {table} for {col}...")
    cursor.execute(f"PRAGMA table_info({table})")
    cols = [r[1] for r in cursor.fetchall()]
    
    if col in cols:
        print(f"Dropping {col} from {table}...")
        try:
            # We must drop triggers first because they Reference the column we are about to drop
            # Actually, modern SQLite might handle it or error out. 
            # Safer to drop triggers first manually or just let the error happen and fix it?
            # The migrate script drops triggers before recreating them.
            # But we need to drop the column BEFORE running migrate, ensuring the triggers don't block the drop.
            
            # Let's drop triggers first to be safe
            print("Dropping associated triggers first...")
            cursor.execute("DROP TRIGGER IF EXISTS sync_nifty_1h_insert")
            cursor.execute("DROP TRIGGER IF EXISTS sync_nifty_1h_update")
            
            cursor.execute(f"ALTER TABLE {table} DROP COLUMN {col}")
            print("Column dropped successfully.")
            
        except sqlite3.OperationalError as e:
            print(f"Failed to drop column: {e}")
            conn.close()
            return
    else:
        print(f"Column {col} not found in {table}.")
        
    conn.commit()
    conn.close()
    
    print("Regenerating triggers (migrate)...")
    migrate_features_merged.migrate()
    print("Done.")

if __name__ == "__main__":
    drop_daily_target()
