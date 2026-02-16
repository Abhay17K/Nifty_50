import pandas as pd
import pandas_ta as ta
import numpy as np

def calculate_hourly_indicators(df):
    """
    Calculates detailed technical indicators for hourly data.
    """
    if df.empty or len(df) < 50: # Need enough data for EMAs
        return df

    # RSI
    df['rsi_14'] = ta.rsi(df['close'], length=14)
    df['rsi_sma_14'] = ta.sma(df['rsi_14'], length=14)
    df['rsi_diff'] = df['rsi_14'] - df['rsi_sma_14']
    df['rsi_slope'] = df['rsi_14'].diff(3) # Slope over 3 bars
    df['rsi_dist_50'] = df['rsi_14'] - 50
    df['rsi_zone'] = np.where(df['rsi_14'] > 70, 'Overbought', 
                             np.where(df['rsi_14'] < 30, 'Oversold', 'Neutral'))

    # ROC Suite
    df['roc_7'] = ta.roc(df['close'], length=7)
    df['roc_9'] = ta.roc(df['close'], length=9)
    df['roc_21'] = ta.roc(df['close'], length=21)
    df['roc7_flag'] = (df['roc_7'] > 0).astype(int)
    df['roc_accel'] = df['roc_7'] - df['roc_21']

    # Range Metrics
    df['hl_range'] = df['high'] - df['low']
    df['range_pct'] = (df['hl_range'] / df['close']) * 100

    # Moving Averages
    df['ema_7'] = ta.ema(df['close'], length=7)
    df['ema_9'] = ta.ema(df['close'], length=9)
    df['ema_20'] = ta.ema(df['close'], length=20)
    df['ema_50'] = ta.ema(df['close'], length=50)
    df['ema_100'] = ta.ema(df['close'], length=100)
    df['sma_25'] = ta.sma(df['close'], length=25)
    
    # LSMA (Least Squares Moving Average)
    df['lsma_25'] = ta.linreg(df['close'], length=25)
    
    # LSMA Logic
    df['close_gt_lsma'] = (df['close'] > df['lsma_25']).astype(int)
    df['close_lt_lsma'] = (df['close'] < df['lsma_25']).astype(int)
    df['close_pct_lsma'] = (df['close'] - df['lsma_25']) / df['lsma_25'] * 100
    df['lsma_diff'] = df['close'] - df['lsma_25']
    df['close_pct_sma_25'] = (df['close'] - df['sma_25']) / df['sma_25'] * 100
    
    # EMA Alignment Flag
    def check_alignment(row):
        if pd.isna(row['ema_100']): return None
        if row['ema_20'] > row['ema_50'] > row['ema_100']: return 'BULLISH'
        if row['ema_20'] < row['ema_50'] < row['ema_100']: return 'BEARISH'
        return 'MIXED'
    
    df['ema_alignment'] = df.apply(check_alignment, axis=1)

    # Bollinger Bands
    bb = ta.bbands(df['close'], length=20, std=2)
    df['bb_upper'] = bb['BBU_20_2.0']
    df['bb_lower'] = bb['BBL_20_2.0']
    df['bb_middle'] = bb['BBM_20_2.0']
    df['bb_width'] = (df['bb_upper'] - df['bb_lower']) / df['bb_middle']
    # Squeeze: width is low compared to historical
    df['bb_squeeze'] = (df['bb_width'] < ta.sma(df['bb_width'], length=20)).astype(int)
    df['bb_position'] = (df['close'] - df['bb_lower']) / (df['bb_upper'] - df['bb_lower'])
    
    # New absolute BB metrics
    df['bb_range'] = df['bb_upper'] - df['bb_lower']
    df['bb_upper_slope'] = df['bb_upper'].diff()
    df['bb_lower_slope'] = df['bb_lower'].diff()

    # ATR
    df['atr_14'] = ta.atr(df['high'], df['low'], df['close'], length=14)
    df['atr_pct'] = df['atr_14'] / df['close'] * 100

    # Breakouts
    df['break_high_5'] = (df['close'] > df['high'].shift(1).rolling(5).max()).astype(int)
    df['break_low_5'] = (df['close'] < df['low'].shift(1).rolling(5).min()).astype(int)

    return df

def calculate_daily_indicators(df):
    """
    Calculates technical indicators for daily data.
    """
    if df.empty or len(df) < 50:
        return df

    # Daily RSI14
    df['rsi_14'] = ta.rsi(df['close'], length=14)
    df['rsi_slope'] = df['rsi_14'].diff(3)

    # Daily EMA20
    df['ema_20'] = ta.ema(df['close'], length=20)
    df['ema_20_slope'] = df['ema_20'].diff(3)

    # Daily Trend Flag
    df['trend_flag'] = np.where(df['close'] > df['ema_20'], 'BULLISH', 'BEARISH')

    return df
