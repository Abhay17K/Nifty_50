import database
import pandas as pd

def debug_signals():
    print("Fetching hourly data...")
    df = database.get_data('1h', limit=1000)
    print(f"Total rows fetched: {len(df)}")
    
    # Check for duplicates or weird indices
    if df.index.duplicated().any():
        print("WARNING: Duplicate indices found!")
        print(df.index[df.index.duplicated()])
        
    df = df.sort_index()
    
    df['f1'] = df['close'].shift(-1)
    df['f2'] = df['close'].shift(-2)
    df['f3'] = df['close'].shift(-3)
    
    last_40 = df.tail(40)
    print("\nLast 40 rows check:")
    print(last_40[['close', 'f1', 'f2', 'f3', 'target']])
    
    # Check if we have data after Feb 10
    after_feb10 = df[df.index >= '2026-02-10']
    print(f"\nRows after Feb 10: {len(after_feb10)}")
    
    # Recalculate and see if it fixes it
    df['new_target'] = ((df['f3'] - df['close']) / df['close']).apply(
        lambda x: 'CALL' if x > 0.004 else ('PUT' if x < -0.004 else 'SIDEWAYS') if pd.notna(x) else None
    )
    
    print("\nRecalculated targets for last 10 rows:")
    print(df[['close', 'f3', 'new_target']].tail(10))

if __name__ == "__main__":
    debug_signals()
