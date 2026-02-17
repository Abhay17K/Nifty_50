import sqlite3

def drop_vol_cols():
    conn = sqlite3.connect('nifty50_data.db')
    cursor = conn.cursor()
    
    # Get all tables
    cursor.execute("SELECT name FROM sqlite_master WHERE type='table'")
    tables = [row[0] for row in cursor.fetchall()]
    
    target_cols = ['vol_avg_20', 'vol_rel_avg', 'vol_rek_avg'] # Including typo just in case
    
    print(f"Scanning tables to drop columns: {target_cols}...")
    
    with open('drop_log.txt', 'w', encoding='utf-8') as f:
        # First, drop known triggers that might block column removal
        triggers_to_drop = ['sync_nifty_1h_insert', 'sync_nifty_1h_update']
        for trig in triggers_to_drop:
            try:
                f.write(f"Dropping trigger {trig}...\n")
                cursor.execute(f"DROP TRIGGER IF EXISTS {trig}")
                f.write("  Success.\n")
            except Exception as e:
                f.write(f"  Failed to drop trigger {trig}: {e}\n")
        
        for table in tables:
            # Check columns
            cursor.execute(f"PRAGMA table_info({table})")
            cols = [row[1] for row in cursor.fetchall()]
            
            to_drop = [c for c in cols if c in target_cols]
            
            if to_drop:
                f.write(f"Table '{table}' has columns to drop: {to_drop}\n")
                for col in to_drop:
                    try:
                        f.write(f"  Dropping {col} from {table}...\n")
                        cursor.execute(f"ALTER TABLE {table} DROP COLUMN {col}")
                        f.write(f"  Success.\n")
                    except sqlite3.OperationalError as e:
                        f.write(f"  Failed (DROP COLUMN might not be supported or other error): {e}\n")
            else:
                 pass
                
    conn.commit()
    conn.close()
    
    # Re-create triggers using the migration script (which will now see the updated schema)
    print("Re-creating triggers...")
    import migrate_features_merged
    try:
        migrate_features_merged.migrate()
        print("Triggers re-created successfully.")
    except Exception as e:
        print(f"Failed to re-create triggers: {e}")
        
    print("Cleanup complete. Log written to drop_log.txt")

if __name__ == "__main__":
    drop_vol_cols()
