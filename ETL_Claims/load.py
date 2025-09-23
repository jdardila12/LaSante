import os
import pandas as pd
from config import EXPORT_PATH

def save_to_csv(df: pd.DataFrame, name: str, max_rows: int = 2000):
    """
    Save a DataFrame to CSV files in chunks of max_rows.
    Files go to EXPORT_PATH/name_partX.csv
    """
    total_rows = len(df)
    num_parts = (total_rows // max_rows) + 1

    for i in range(num_parts):
        chunk = df.iloc[i*max_rows:(i+1)*max_rows]
        if not chunk.empty:
            file_path = os.path.join(EXPORT_PATH, f"{name}_part{i+1}.csv")
            chunk.to_csv(file_path, index=False, encoding="utf-8-sig")
            print(f"✅ Saved: {file_path} ({len(chunk)} rows)")
