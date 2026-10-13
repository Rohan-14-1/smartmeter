"""
Train an IsolationForest anomaly-detection model on the
Global_active_power column and save it as anomaly_model.pkl.

Run once (offline):
    python train_model.py
"""
import os
import pandas as pd
import joblib
from sklearn.ensemble import IsolationForest

DATA_DIR = os.path.dirname(os.path.abspath(__file__))
CSV_PATH = os.path.join(DATA_DIR, "data.csv")
MODEL_PATH = os.path.join(DATA_DIR, "anomaly_model.pkl")


def train():
    print("📖  Loading data …")
    df = pd.read_csv(CSV_PATH, usecols=['Global_active_power'])
    df.dropna(inplace=True)
    print(f"   {len(df):,} rows loaded.")

    print("🏋️  Training IsolationForest (contamination=0.01) …")
    model = IsolationForest(
        contamination=0.01,
        n_estimators=200,
        random_state=42,
        n_jobs=-1,
    )
    model.fit(df[['Global_active_power']].values)

    joblib.dump(model, MODEL_PATH)
    print(f"✔  Model saved to {MODEL_PATH}")


if __name__ == "__main__":
    train()
    print("\n✅  Training complete!")
