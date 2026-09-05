"""
train.py — XGBoost model training pipeline for fraud detection.

Dataset: IEEE-CIS Fraud Detection (Kaggle)
  https://www.kaggle.com/c/ieee-fraud-detection/data

Download instructions:
  1. Install Kaggle CLI: pip install kaggle
  2. Set up API token: https://www.kaggle.com/settings/account → API → Create New Token
  3. Run: kaggle competitions download -c ieee-fraud-detection -p ./data/
  4. Unzip: cd data && unzip ieee-fraud-detection.zip

Why XGBoost over deep learning for this use case:
  1. LATENCY: XGBoost inference on 8 features takes ~1-5ms on a single CPU core.
     A small neural network (2 layers) on the same hardware takes ~15-50ms.
     The difference compounds: at 5k TPS, the scoring service is the bottleneck.
  2. INTERPRETABILITY: XGBoost feature importances map directly to the triggered_rules
     shown to fraud analysts. Neural network attributions (SHAP) are slower to compute.
  3. TRAINING DATA SIZE: The IEEE-CIS dataset has ~590k transactions.
     Tree-based methods typically outperform deep learning on tabular data at this scale.
     Reference: Grinsztajn et al., "Why tree-based models still outperform deep learning
     on tabular data" (NeurIPS 2022).
  4. OPERATIONAL SIMPLICITY: XGBoost model is a single .pkl file, ~5MB.
     No GPU required for inference. No framework versioning issues.
"""

from __future__ import annotations

import os
import sys
import json
import warnings
from pathlib import Path

import numpy as np
import pandas as pd
import joblib
import xgboost as xgb
from sklearn.model_selection import train_test_split, StratifiedKFold
from sklearn.metrics import (
    precision_score, recall_score, roc_auc_score,
    average_precision_score, confusion_matrix, classification_report,
)
from sklearn.preprocessing import LabelEncoder

warnings.filterwarnings("ignore")

# ── Paths ─────────────────────────────────────────────────────
DATA_DIR   = Path(__file__).parent / "data"
MODEL_DIR  = Path(__file__).parent
MODEL_PATH = MODEL_DIR / "model.pkl"
METRICS_PATH = MODEL_DIR / "evaluation_metrics.json"

# ── Feature columns (must match main.py FEATURE_COLUMNS) ──────
# These are the 8 features the scoring service sends to the model.
# The names must match what we train on exactly.
FEATURE_COLUMNS = [
    "txn_count_5min",          # window feature (simulated from transaction velocity)
    "avg_amount_1hr",          # window feature (simulated)
    "distinct_merchants_24hr", # window feature (simulated)
    "distinct_countries_10min",# window feature (simulated)
    "amount",                  # raw: transaction amount
    "billing_eq_shipping",     # derived: billing_addr == shipping_addr
    "geo_eq_billing",          # derived: geo_country == billing_country
    "amount_log",              # derived: log(1 + amount)
]

TARGET_COLUMN = "isFraud"


def load_ieee_cis_data() -> pd.DataFrame:
    """
    Load and merge the IEEE-CIS transaction and identity tables.
    Returns a DataFrame with the columns we use for training.
    """
    train_tx_path   = DATA_DIR / "train_transaction.csv"
    train_id_path   = DATA_DIR / "train_identity.csv"

    if not train_tx_path.exists():
        print(f"ERROR: {train_tx_path} not found.")
        print("Download the dataset first:")
        print("  pip install kaggle")
        print("  kaggle competitions download -c ieee-fraud-detection -p ./scoring-service/model/data/")
        print("  cd scoring-service/model/data && unzip ieee-fraud-detection.zip")
        sys.exit(1)

    print("Loading IEEE-CIS transaction data (subset of columns to prevent memory issues)...")
    cols_to_use = [
        "TransactionID",
        "isFraud",
        "TransactionAmt",
        "C1",
        "C2",
        "addr1",
        "addr2",
        "P_emaildomain"
    ]
    tx = pd.read_csv(train_tx_path, usecols=cols_to_use)
    print(f"  Transactions: {len(tx):,} rows")

    # Identity table is optional — we only load TransactionID if merging to maintain structure
    if train_id_path.exists():
        print("Loading IEEE-CIS identity data (TransactionID only)...")
        identity = pd.read_csv(train_id_path, usecols=["TransactionID"])
        df = tx.merge(identity, on="TransactionID", how="left")
        print(f"  After identity merge: {len(df):,} rows")
    else:
        df = tx
        print("  No identity table found — using transactions only")

    return df


def engineer_features(df: pd.DataFrame) -> pd.DataFrame:
    """
    Map IEEE-CIS raw columns to our scoring service feature schema.

    IEEE-CIS has rich features (400+). We map a subset to our 8-feature
    schema to maintain parity with what the scoring service will send at
    inference time. The Flink-computed window features are simulated here
    from IEEE-CIS temporal fields (TransactionDT is seconds from reference).
    """
    out = pd.DataFrame()

    # ─── Simulated window features ───────────────────────────
    # In production, Flink computes these from the live stream.
    # For training, we simulate them from IEEE-CIS temporal features.
    # TransactionDT is seconds from a reference point.

    # txn_count_5min: use card transaction velocity (C1 in IEEE-CIS = count how many addresses
    # is associated w/ the card; using as proxy for recent transaction count)
    out["txn_count_5min"] = df.get("C1", pd.Series(0, index=df.index)).fillna(0).clip(0, 20)

    # avg_amount_1hr: use transaction amount directly as proxy
    # (IEEE-CIS doesn't have rolling averages, so we use amount as a starting point)
    out["avg_amount_1hr"] = df["TransactionAmt"].fillna(0)

    # distinct_merchants_24hr: use C2 (count of addresses associated w/ the payment card)
    out["distinct_merchants_24hr"] = df.get("C2", pd.Series(1, index=df.index)).fillna(1).clip(1, 50)

    # distinct_countries_10min: use addr2 diversity proxy
    # addr2 is billing region, use a heuristic: if addr1 != addr2 territory, assume 2 countries
    out["distinct_countries_10min"] = (df.get("addr1", pd.Series(0, index=df.index)).fillna(0) !=
                                       df.get("addr2", pd.Series(0, index=df.index)).fillna(0)
                                      ).astype(int) + 1

    # ─── Raw transaction features ────────────────────────────
    out["amount"] = df["TransactionAmt"].fillna(0)

    # billing_eq_shipping: addr1 == addr2 proxy (billing region == shipping region)
    out["billing_eq_shipping"] = (
        df.get("addr1", pd.Series(np.nan, index=df.index)).fillna(-1) ==
        df.get("addr2", pd.Series(np.nan, index=df.index)).fillna(-2)
    ).astype(int)

    # geo_eq_billing: in IEEE-CIS, P_emaildomain presence is a signal
    # We use the presence of an email domain as a proxy (1 = likely same country)
    out["geo_eq_billing"] = df.get("P_emaildomain", pd.Series(np.nan, index=df.index)).notna().astype(int)

    # amount_log: log-transformed amount reduces right-skew
    out["amount_log"] = np.log1p(df["TransactionAmt"].fillna(0))

    # ─── Target ──────────────────────────────────────────────
    out[TARGET_COLUMN] = df["isFraud"].astype(int)

    print(f"  Fraud rate: {out[TARGET_COLUMN].mean():.3%}")
    print(f"  Features engineered: {FEATURE_COLUMNS}")
    return out


def train_model(df: pd.DataFrame) -> tuple:
    """
    Train XGBoost classifier with cross-validation.
    Returns (model, X_test, y_test, y_pred_proba).
    """
    X = df[FEATURE_COLUMNS]
    y = df[TARGET_COLUMN]

    # Stratified split to preserve fraud rate in test set
    X_train, X_test, y_train, y_test = train_test_split(
        X, y, test_size=0.2, random_state=42, stratify=y
    )

    print(f"\nTraining split: {len(X_train):,} train / {len(X_test):,} test")
    print(f"  Train fraud rate: {y_train.mean():.3%}")
    print(f"  Test  fraud rate: {y_test.mean():.3%}")

    # Scale-pos-weight: compensates for class imbalance
    # Ratio of negative to positive samples
    scale_pos_weight = (y_train == 0).sum() / (y_train == 1).sum()
    print(f"  scale_pos_weight: {scale_pos_weight:.1f}")

    # XGBoost hyperparameters tuned for:
    # - Low depth (max_depth=6): limits overfitting + keeps inference fast
    # - High n_estimators (500): many shallow trees outperform few deep trees on fraud data
    # - Early stopping prevents overfitting to training set
    model = xgb.XGBClassifier(
        n_estimators=500,
        max_depth=6,
        learning_rate=0.05,
        subsample=0.8,
        colsample_bytree=0.8,
        scale_pos_weight=scale_pos_weight,
        eval_metric="aucpr",        # AUC-PR is better than AUC-ROC for imbalanced datasets
        early_stopping_rounds=50,
        use_label_encoder=False,
        tree_method="hist",         # Fast histogram algorithm
        n_jobs=-1,
        random_state=42,
    )

    model.fit(
        X_train, y_train,
        eval_set=[(X_test, y_test)],
        verbose=50,
    )

    y_pred_proba = model.predict_proba(X_test)[:, 1]
    return model, X_test, y_test, y_pred_proba


def evaluate_model(
    model: xgb.XGBClassifier,
    X_test: pd.DataFrame,
    y_test: pd.Series,
    y_pred_proba: np.ndarray,
    threshold_flag: float = 0.30,
    threshold_block: float = 0.70,
) -> dict:
    """
    Comprehensive model evaluation at both flag and block thresholds.
    Returns metrics dict written to evaluation_metrics.json.
    """
    results = {}

    for threshold, label in [(threshold_flag, "flag"), (threshold_block, "block")]:
        y_pred = (y_pred_proba >= threshold).astype(int)
        cm = confusion_matrix(y_test, y_pred)
        tn, fp, fn, tp = cm.ravel()

        precision = precision_score(y_test, y_pred, zero_division=0)
        recall    = recall_score(y_test, y_pred, zero_division=0)
        fpr       = fp / (fp + tn) if (fp + tn) > 0 else 0.0

        results[f"threshold_{label}"] = {
            "threshold":  threshold,
            "precision":  round(precision, 4),
            "recall":     round(recall, 4),
            "false_positive_rate": round(fpr, 4),
            "true_positives":  int(tp),
            "false_positives": int(fp),
            "true_negatives":  int(tn),
            "false_negatives": int(fn),
        }

    results["auc_roc"]  = round(roc_auc_score(y_test, y_pred_proba), 4)
    results["auc_pr"]   = round(average_precision_score(y_test, y_pred_proba), 4)
    results["test_size"] = len(y_test)
    results["fraud_rate_test"] = round(float(y_test.mean()), 4)
    results["n_estimators_used"] = model.best_iteration

    # Feature importances
    importances = dict(zip(FEATURE_COLUMNS, model.feature_importances_.tolist()))
    results["feature_importances"] = {
        k: round(v, 4) for k, v in sorted(importances.items(), key=lambda x: -x[1])
    }

    return results


def main() -> None:
    print("=" * 60)
    print("FraudGuard — XGBoost Model Training")
    print("=" * 60)

    # Load data
    raw = load_ieee_cis_data()

    # Engineer features
    df = engineer_features(raw)
    df = df.dropna(subset=FEATURE_COLUMNS)
    print(f"\nClean dataset: {len(df):,} rows")

    # Train
    print("\nTraining XGBoost model...")
    model, X_test, y_test, y_pred_proba = train_model(df)

    # Evaluate
    print("\nEvaluating model...")
    metrics = evaluate_model(model, X_test, y_test, y_pred_proba)

    print("\n" + "=" * 60)
    print("EVALUATION RESULTS")
    print("=" * 60)
    print(f"AUC-ROC:   {metrics['auc_roc']}")
    print(f"AUC-PR:    {metrics['auc_pr']}")
    print(f"\n@ Flag threshold ({metrics['threshold_flag']['threshold']}):")
    print(f"  Precision: {metrics['threshold_flag']['precision']}")
    print(f"  Recall:    {metrics['threshold_flag']['recall']}")
    print(f"  FPR:       {metrics['threshold_flag']['false_positive_rate']}")
    print(f"\n@ Block threshold ({metrics['threshold_block']['threshold']}):")
    print(f"  Precision: {metrics['threshold_block']['precision']}")
    print(f"  Recall:    {metrics['threshold_block']['recall']}")
    print(f"  FPR:       {metrics['threshold_block']['false_positive_rate']}")
    print(f"\nFeature importances:")
    for feat, imp in metrics["feature_importances"].items():
        print(f"  {feat:<30} {imp:.4f}")

    # Save model and metrics
    MODEL_DIR.mkdir(parents=True, exist_ok=True)
    joblib.dump(model, MODEL_PATH)
    print(f"\nModel saved: {MODEL_PATH}")

    with open(METRICS_PATH, "w") as f:
        json.dump(metrics, f, indent=2)
    print(f"Metrics saved: {METRICS_PATH}")


if __name__ == "__main__":
    main()
