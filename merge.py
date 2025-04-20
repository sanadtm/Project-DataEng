import os
import json
from collections import defaultdict

DATA_FOLDER = "/opt/Project-DataEng/vehicle_data/"
MERGE_FOLDER = os.path.join(DATA_FOLDER, "merged_data")
grouped_files = defaultdict(list)

# Create merge output folder if it doesn't exist
os.makedirs(MERGE_FOLDER, exist_ok=True)

# Group filenames by date
for fname in os.listdir(DATA_FOLDER):
    if fname.endswith(".json") and "_" in fname:
        date = fname.split("_")[0]
        grouped_files[date].append(fname)

# Merge and move
for date, files in grouped_files.items():
    merged = []
    for fname in files:
        path = os.path.join(DATA_FOLDER, fname)
        try:
            with open(path, "r") as f:
                data = json.load(f)
                merged.extend(data)
        except Exception as e:
            print(f"Skipping {fname} (error: {e})")

    merged_filename = f"{date}.json"
    merged_path = os.path.join(MERGE_FOLDER, merged_filename)
    with open(merged_path, "w") as f:
        json.dump(merged, f, indent=2)

    print(f"{date}: Merged {len(merged)} records to {merged_path}")
