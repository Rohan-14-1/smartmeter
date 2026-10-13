"""
Download and prepare the UCI Household Electric Power Consumption dataset.
- Downloads the ZIP from the UCI archive
- Merges Date and Time columns into Date_Time
- Replaces '?' missing values and drops NaN rows
- Saves a clean data.csv
"""
import os
import zipfile
import urllib.request
import pandas as pd

DATA_DIR = os.path.dirname(os.path.abspath(__file__))
ZIP_URL = "https://archive.ics.uci.edu/ml/machine-learning-databases/00235/household_power_consumption.zip"
ZIP_PATH = os.path.join(DATA_DIR, "household_power_consumption.zip")
TXT_NAME = "household_power_consumption.txt"
CSV_OUT  = os.path.join(DATA_DIR, "data.csv")

def download_dataset():
    """Download the ZIP file if it doesn't exist locally."""
    if os.path.exists(CSV_OUT):
        print(f"✔  {CSV_OUT} already exists — skipping download.")
        return
    if not os.path.exists(ZIP_PATH):
        print("⬇  Downloading dataset from UCI archive …")
        urllib.request.urlretrieve(ZIP_URL, ZIP_PATH)
        print(f"✔  Saved to {ZIP_PATH}")
    else:
        print(f"✔  ZIP already present at {ZIP_PATH}")

def extract_and_clean():
    """Extract the ZIP, clean the data, and write data.csv."""
    if os.path.exists(CSV_OUT):
        print(f"✔  {CSV_OUT} already exists — nothing to do.")
        return

    # Extract the text file from the ZIP
    print("📦  Extracting ZIP …")
    with zipfile.ZipFile(ZIP_PATH, 'r') as zf:
        zf.extract(TXT_NAME, DATA_DIR)
    txt_path = os.path.join(DATA_DIR, TXT_NAME)

    # Read with ';' separator
    print("🔧  Reading and cleaning data …")
    df = pd.read_csv(
        txt_path,
        sep=';',
        low_memory=False,
        na_values='?',
    )

    # Merge Date + Time into a single Date_Time column
    df['Date_Time'] = df['Date'].astype(str) + ' ' + df['Time'].astype(str)
    df.drop(columns=['Date', 'Time'], inplace=True)

    # Re-order so Date_Time comes first
    cols = ['Date_Time'] + [c for c in df.columns if c != 'Date_Time']
    df = df[cols]

    # Drop rows with missing values
    df.dropna(inplace=True)

    # Ensure numeric columns are actually numeric
    numeric_cols = [c for c in df.columns if c != 'Date_Time']
    for col in numeric_cols:
        df[col] = pd.to_numeric(df[col], errors='coerce')
    df.dropna(inplace=True)

    # Save to CSV
    df.to_csv(CSV_OUT, index=False)
    print(f"✔  Saved {len(df):,} rows to {CSV_OUT}")

    # Clean up the raw text file (optional)
    os.remove(txt_path)
    print("🗑  Removed raw text file.")

if __name__ == "__main__":
    download_dataset()
    extract_and_clean()
    print("\n✅  Dataset preparation complete!")
