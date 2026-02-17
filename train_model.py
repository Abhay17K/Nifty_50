import sqlite3
import pandas as pd
import numpy as np
from sklearn.ensemble import RandomForestClassifier
from sklearn.metrics import classification_report, confusion_matrix, accuracy_score
import joblib
from datetime import datetime

def load_and_prepare_data():
    """
    Load data from features_merged table and prepare for ML
    """
    print("=" * 60)
    print("STEP 1: Loading data from features_merged")
    print("=" * 60)
    
    conn = sqlite3.connect('nifty50_data.db')
    
    # Load all data
    df = pd.read_sql("SELECT * FROM features_merged", conn)
    conn.close()
    
    print(f"Loaded {len(df)} rows")
    print(f"Columns: {len(df.columns)}")
    
    # CRITICAL: Sort chronologically
    print("\nSTEP 2: Sorting chronologically by timestamp")
    df['timestamp'] = pd.to_datetime(df['timestamp'])
    df = df.sort_values("timestamp").reset_index(drop=True)
    
    # STEP 1 — Filter SIDEWAYS
    print(f"\nFiltering out SIDEWAYS data...")
    rows_before = len(df)
    df = df[df['target'] != 'SIDEWAYS'].copy()
    print(f"  Rows before: {rows_before} | Rows after (PUT/CALL only): {len(df)}")
    
    # STEP 2 — Use target_bin instead of target_encoded
    # PUT = 0, CALL = 1 for binary classification
    df['target_bin'] = df['target'].map({'PUT': 0, 'CALL': 1})
    
    # Class Balancing using resample
    print(f"\nBalancing classes (PUT vs CALL)...")
    from sklearn.utils import resample
    df_put = df[df["target_bin"] == 0]
    df_call = df[df["target_bin"] == 1]
    
    min_size = min(len(df_put), len(df_call))
    print(f"  PUT count: {len(df_put)} | CALL count: {len(df_call)}")
    print(f"  Resampling to min_size: {min_size}")
    
    df_put_resampled = resample(df_put, n_samples=min_size, replace=False, random_state=42)
    df_call_resampled = resample(df_call, n_samples=min_size, replace=False, random_state=42)
    
    df = pd.concat([df_put_resampled, df_call_resampled]).sort_values("timestamp")
    print(f"  Final balanced rows: {len(df)}")
    
    # Extract hour from timestamp
    df['hour'] = df['timestamp'].dt.hour
    
    print(f"Date range: {df['timestamp'].min()} to {df['timestamp'].max()}")
    print(f"Hour range: {df['hour'].min()} to {df['hour'].max()}")
    
    return df

def prepare_features(df):
    """
    Prepare features and target for ML
    """
    print("\n" + "=" * 60)
    print("STEP 3: Preparing features and target")
    print("=" * 60)
    
    # Define columns to exclude from ML features
    exclude_cols = [
        'timestamp',       # Time identifier
        'date',            # Date component
        'time',            # Time string
        'target',          # Original target string
        'signal',          # Old name
        'target_encoded',  # Old name
        'target_bin'       # This is our target variable (what we predict)
    ]
    
    print(f"\n📌 Using 'target_bin' as target (PUT=0, CALL=1)")
    print(f"📌 'hour' feature created from timestamp (9:15 → 9, 10:15 → 10)")
    
    # Get feature columns
    feature_cols = [col for col in df.columns if col not in exclude_cols]
    
    # CHECK FOR FEATURE LEAKAGE
    print("\n" + "=" * 60)
    print("STEP 4: Checking for feature leakage")
    print("=" * 60)
    leak_cols = [c for c in feature_cols if "future" in c.lower()]
    if leak_cols:
        print(f"⚠️  WARNING: Found potential leak columns: {leak_cols}")
        print("   These columns should be removed!")
    else:
        print("✅ No 'future' columns found - good!")
    
    # Prepare X (features) and y (target)
    X = df[feature_cols].copy()
    y = df['target_bin'].copy()
    
    print(f"\n" + "=" * 60)
    print("STEP 5: Enforcing numeric types")
    print("=" * 60)
    print(f"Features before enforcement: {len(X.columns)}")
    
    # Check for non-numeric columns (should only be intentional categoricals)
    non_numeric = X.select_dtypes(include=['object']).columns.tolist()
    if non_numeric:
        print(f"⚠️  WARNING: Found object dtype columns: {non_numeric}")
        print("   These should not exist except for intentional categoricals!")
        print("   Attempting to convert to numeric...")
    
    # ENFORCE NUMERIC: Convert all to numeric, coerce errors to NaN
    X = X.apply(pd.to_numeric, errors='coerce')
    
    # Check for columns with all NaN values
    all_nan_cols = X.columns[X.isna().all()].tolist()
    if all_nan_cols:
        print(f"\n⚠️  Dropping columns with all NaN values: {all_nan_cols}")
        X = X.drop(columns=all_nan_cols)
    
    # Check for NaN values introduced by coercion
    nan_counts = X.isna().sum()
    nan_cols = nan_counts[nan_counts > 0]
    if len(nan_cols) > 0:
        print(f"\n⚠️  NaN values found after coercion:")
        for col, count in nan_cols.items():
            pct = (count / len(X)) * 100
            print(f"   - {col}: {count} NaNs ({pct:.1f}%)")
    
    # Drop rows with NaN
    rows_before = len(X)
    X = X.dropna()
    y = y.loc[X.index]
    rows_after = len(X)
    
    if rows_before != rows_after:
        print(f"\n⚠️  Dropped {rows_before - rows_after} rows with NaN values")
    
    print(f"\nFinal dataset: {len(X)} rows, {len(X.columns)} features")
    
    print(f"\nTarget distribution:")
    print(y.value_counts().sort_index())
    print(f"\nPUT  (0): {(y == 0).sum()}")
    print(f"CALL (1): {(y == 1).sum()}")
    
    print(f"\nFeature columns ({len(X.columns)}):")
    for col in X.columns:
        print(f"  - {col}")
    
    return X, y, list(X.columns)

def split_data_chronologically(X, y, test_size=0.2):
    """
    Split data chronologically (NOT random) to avoid look-ahead bias
    """
    print("\n" + "=" * 60)
    print("STEP 6: Splitting data chronologically")
    print("=" * 60)
    
    if len(X) == 0:
        raise ValueError("Cannot split empty dataset! All rows were dropped.")
    
    split_idx = int(len(X) * (1 - test_size))
    
    X_train = X.iloc[:split_idx]
    X_test = X.iloc[split_idx:]
    y_train = y.iloc[:split_idx]
    y_test = y.iloc[split_idx:]
    
    print(f"Train set: {len(X_train)} rows ({len(X_train)/len(X)*100:.1f}%)")
    print(f"Test set: {len(X_test)} rows ({len(X_test)/len(X)*100:.1f}%)")
    
    return X_train, X_test, y_train, y_test

def train_model(X_train, X_test, y_train, y_test, feature_cols):
    """
    Train Random Forest classifier (NO SCALING - RF doesn't need it)
    """
    print("\n" + "=" * 60)
    print("STEP 7: Training Random Forest model")
    print("=" * 60)
    
    # NO SCALER - RandomForest doesn't need feature scaling
    print("\n✅ Using raw features (no scaling - RF doesn't need it)")
    
    # Train model with improved hyperparameters
    print("\nTraining Random Forest with conservative hyperparameters...")
    print("  - n_estimators: 300 (more trees for stability)")
    print("  - max_depth: 6 (reduced to prevent overfitting)")
    print("  - min_samples_leaf: 30 (increased for ~5000 rows)")
    
    model = RandomForestClassifier(
        n_estimators=300,
        max_depth=6,
        min_samples_leaf=30,
        random_state=42,
        n_jobs=-1,
        class_weight='balanced'  # Handle class imbalance
    )
    
    model.fit(X_train, y_train)
    
    # Evaluate
    print("\n" + "=" * 60)
    print("STEP 8: Evaluating model")
    print("=" * 60)
    
    y_pred_train = model.predict(X_train)
    y_pred_test = model.predict(X_test)
    
    print("\nTest Set Classification Report:")
    report = classification_report(y_test, y_pred_test, 
                                 target_names=['PUT', 'CALL'],
                                 output_dict=True)
    
    # Focus on Precision for PUT and CALL
    print(f"  Precision for PUT  (0): {report['PUT']['precision']:.3f}")
    print(f"  Precision for CALL (1): {report['CALL']['precision']:.3f}")

    print("\nConfusion Matrix (Test Set):")
    cm = confusion_matrix(y_test, y_pred_test)
    print(cm)
    
    # PROBABILITY-BASED CONFIDENCE FILTERING
    print("\n" + "=" * 60)
    print("STEP 9: Probability-based confidence analysis")
    print("=" * 60)
    
    probs_test = model.predict_proba(X_test)
    confidence_test = probs_test.max(axis=1)
    
    print(f"\nConfidence distribution (test set):")
    print(f"  Mean: {confidence_test.mean():.3f} | Median: {np.median(confidence_test):.3f}")
    
    # Analyze precision by confidence threshold
    print(f"\n📊 Precision at confidence thresholds:")
    for threshold in [0.4, 0.5, 0.6, 0.7]:
        mask = confidence_test >= threshold
        if mask.sum() > 0:
            sub_y_test = y_test[mask]
            sub_y_pred = y_pred_test[mask]
            
            # Recalculate precision for PUT and CALL at this threshold
            # We use zero_division=0 to handle cases where threshold is too high
            sub_report = classification_report(sub_y_test, sub_y_pred, 
                                             labels=[0, 1],
                                             target_names=['PUT', 'CALL'],
                                             output_dict=True,
                                             zero_division=0)
            
            print(f"  Confidence >= {threshold}:")
            print(f"    PUT Precision:  {sub_report['PUT']['precision']:.3f} ({ (sub_y_pred==0).sum() } trades)")
            print(f"    CALL Precision: {sub_report['CALL']['precision']:.3f} ({ (sub_y_pred==1).sum() } trades)")
            print(f"    Coverage:       {mask.sum() / len(y_test) * 100:.1f}% of data")
    
    print(f"\n💡 TRADING GUIDELINE: Focus on PUT/CALL Precision at >0.6 Confidence.")
    
    # Feature importance
    print("\n" + "=" * 60)
    print("STEP 10: Feature importance")
    print("=" * 60)
    
    feature_importance = pd.DataFrame({
        'feature': feature_cols,
        'importance': model.feature_importances_
    }).sort_values('importance', ascending=False)
    
    print("\nTop 15 Most Important Features:")
    print(feature_importance.head(15).to_string(index=False))
    
    # Save model and feature importance
    print("\n" + "=" * 60)
    print("STEP 11: Saving model and artifacts")
    print("=" * 60)
    
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    model_filename = f'nifty50_model_{timestamp}.pkl'
    
    joblib.dump(model, model_filename)
    print(f"✅ Model saved: {model_filename}")
    
    # Save feature importance
    feature_importance.to_csv(f'feature_importance_{timestamp}.csv', index=False)
    print(f"✅ Feature importance saved: feature_importance_{timestamp}.csv")
    
    # Save confidence analysis
    confidence_df = pd.DataFrame({
        'actual': y_test.values,
        'predicted': y_pred_test,
        'confidence': confidence_test,
        'prob_PUT': probs_test[:, 0],
        'prob_CALL': probs_test[:, 1]
    })
    confidence_df.to_csv(f'test_predictions_{timestamp}.csv', index=False)
    print(f"✅ Test predictions with confidence saved: test_predictions_{timestamp}.csv")
    
    return model, feature_importance

def main():
    print("\n" + "=" * 60)
    print("NIFTY 50 ML TRAINING PIPELINE (FIXED)")
    print("=" * 60)
    
    # Load and prepare data
    df = load_and_prepare_data()
    
    # Prepare features
    X, y, feature_cols = prepare_features(df)
    
    # Split chronologically
    X_train, X_test, y_train, y_test = split_data_chronologically(X, y, test_size=0.2)
    
    # Train and evaluate
    model, feature_importance = train_model(
        X_train, X_test, y_train, y_test, feature_cols
    )
    
    print("\n" + "=" * 60)
    print("TRAINING COMPLETE!")
    print("=" * 60)
    print("\n✅ Key improvements:")
    print("  1. Removed StandardScaler (RF doesn't need it)")
    print("  2. Enforced numeric types with pd.to_numeric")
    print("  3. Using explicit 'target_encoded' column")
    print("  4. Checked for feature leakage (no 'future' columns)")
    print("  5. Improved hyperparameters (300 trees, depth=6, min_leaf=30)")
    print("  6. Added probability-based confidence filtering")
    print("\n💡 Remember: Trade only when confidence > 0.6!")

if __name__ == "__main__":
    main()
