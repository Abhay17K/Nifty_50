import pandas as pd

def generate_targets(df):
    """
    Takes an hourly OHLC dataframe and adds signals based on future returns.
    """
    # 1. Shift close by 3 rows to create future_close (looking 3 hours ahead)
    df['future_close'] = df['close'].shift(-3)

    # 2. Compute future_return
    df['future_return'] = (df['future_close'] - df['close']) / df['close']

    # 3. Create target column
    def categorize_target(ret):
        if pd.isna(ret):
            return None
        if ret > 0.004:
            return 'CALL'
        elif ret < -0.004:
            return 'PUT'
        else:
            return 'SIDEWAYS'

    df['target'] = df['future_return'].apply(categorize_target)

    # 4. Drop rows with NaN
    df_result = df.dropna(subset=['future_close', 'future_return', 'target']).copy()
    
    return df_result

# Example usage (uncomment and modify to test with your own CSV)
# if __name__ == "__main__":
#     # Load your data
#     # df = pd.read_csv('your_hourly_data.csv', index_col=0, parse_dates=True)
#     
#     # Process data
#     # final_df = generate_targets(df)
#     
#     # Save results
#     # final_df.to_csv('processed_data_with_targets.csv')
#     # print("Finished processing.")
