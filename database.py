import sqlite3
import pandas as pd
from datetime import datetime
import os

DB_NAME = 'nifty50_data.db'

def get_db_connection():
    conn = sqlite3.connect(DB_NAME)
    conn.row_factory = sqlite3.Row
    return conn

def init_db():
    conn = get_db_connection()
    c = conn.cursor()
    
    # Create tables for different timeframes
    timeframes = ['15m', '1h', '1d', '1wk']
    
    for tf in timeframes:
        table_name = f'nifty_{tf}'
        c.execute(f'''
            CREATE TABLE IF NOT EXISTS {table_name} (
                timestamp TEXT PRIMARY KEY,
                open REAL,
                high REAL,
                low REAL,
                close REAL,
                volume INTEGER,
                target TEXT
            )
        ''')
        
    conn.commit()
    conn.close()
    print(f"Database {DB_NAME} initialized successfully.")

def store_data(df, timeframe):
    """
    Store OHLC data and all indicators in the database with safe upserts.
    """
    if df.empty:
        return
        
    conn = get_db_connection()
    table_name = f'nifty_{timeframe}'
    
    df_reset = df.reset_index()
    if 'Date' in df_reset.columns:
        df_reset.rename(columns={'Date': 'timestamp'}, inplace=True)
    elif 'Datetime' in df_reset.columns:
        df_reset.rename(columns={'Datetime': 'timestamp'}, inplace=True)
        
    df_reset['timestamp'] = df_reset['timestamp'].astype(str)
    
    # Get all column names from the database table to see what we can store
    cursor = conn.execute(f"PRAGMA table_info({table_name})")
    table_cols = [row[1] for row in cursor.fetchall()]
    
    # Identify which columns in the DF exist in the table
    cols_to_store = [c for c in df_reset.columns if c in table_cols]
    
    if 'timestamp' not in cols_to_store:
        print(f"Error: timestamp column missing for {timeframe}")
        conn.close()
        return

    data_to_store = df_reset[cols_to_store].copy()
    
    # Prepare the UPSERT query
    col_str = ', '.join(cols_to_store)
    placeholders = ', '.join(['?'] * len(cols_to_store))
    
    # Update all columns EXCEPT timestamp on conflict
    update_cols = [c for c in cols_to_store if c != 'timestamp']
    update_clause = ', '.join([f"{c}=excluded.{c}" for c in update_cols])
    
    query = f'''
        INSERT INTO {table_name} ({col_str})
        VALUES ({placeholders})
        ON CONFLICT(timestamp) DO UPDATE SET
        {update_clause}
    '''
    
    # Use executemany for batch performance
    values = [tuple(x) for x in data_to_store.values]
    conn.executemany(query, values)
        
    conn.commit()
    conn.close()
    print(f"Stored {len(df)} records for {timeframe} timeframe (all columns upserted).")

def get_data(timeframe, start_date=None, end_date=None, limit=None):
    """
    Retrieve data from database.
    start_date, end_date: ISO format strings (YYYY-MM-DD...)
    """
    conn = get_db_connection()
    table_name = f'nifty_{timeframe}'
    
    query = f"SELECT * FROM {table_name}"
    params = []
    
    conditions = []
    if start_date:
        # If start_date is just YYYY-MM-DD, add time
        if len(start_date) == 10:
            start_date += " 00:00:00"
        conditions.append("timestamp >= ?")
        params.append(start_date)
    if end_date:
        # If end_date is just YYYY-MM-DD, add time
        if len(end_date) == 10:
            end_date += " 23:59:59"
        conditions.append("timestamp <= ?")
        params.append(end_date)
        
    if conditions:
        query += " WHERE " + " AND ".join(conditions)
        
    query += " ORDER BY timestamp DESC"
    
    if limit:
        query += f" LIMIT {limit}"
        
    df = pd.read_sql_query(query, conn, params=params)
    conn.close()
    
    if not df.empty:
        df['timestamp'] = pd.to_datetime(df['timestamp'])
        df.set_index('timestamp', inplace=True)
        df.sort_index(ascending=False, inplace=True)
        
    return df

if __name__ == '__main__':
    init_db()
