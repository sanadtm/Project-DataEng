from urllib import request, error  
import csv
import json
import os
from datetime import datetime

# Constants
BASE_URL = "https://busdata.cs.pdx.edu/api/getBreadCrumbs?vehicle_id="
CSV_FILE = "Glitch Vehicle IDs - VehicleGroupsIDs.csv"
OUTPUT_DIR = "vehicle_data"

# Create output directory if it doesn't exist
os.makedirs(OUTPUT_DIR, exist_ok=True)

def get_vehicle_ids(csv_file):
    vehicle_ids = set()
    with open(csv_file, newline='') as csvfile:
        reader = csv.reader(csvfile)
        for row in reader:
            for item in row:
                item = item.strip()
                if item.isdigit():
                    vehicle_ids.add(item)
    return list(vehicle_ids)

def fetch_data(vehicle_id):
    url = f"{BASE_URL}{vehicle_id}"
    try:
        with request.urlopen(url) as response:
            data = json.loads(response.read().decode('utf-8'))
            return data
    except error.HTTPError as e:
        if e.code == 404:
            print(f"[{vehicle_id}] No data found (code 404). Skipping.")
        else:
            print(f"[{vehicle_id}] HTTP error: {e.code}. Skipping.")
    except Exception as e:
        print(f"[{vehicle_id}] General error: {e}. Skipping.")
    return None

def save_json(vehicle_id, data):
    today_str = datetime.now().strftime("%Y-%m-%d")
    filename = f"{OUTPUT_DIR}/{today_str}_{vehicle_id}.json"
    with open(filename, 'w') as f:
        json.dump(data, f, indent=2)
    print(f"[{vehicle_id}] Data saved to {filename}")

def main():
    vehicle_ids = get_vehicle_ids(CSV_FILE)
    print(f"Found {len(vehicle_ids)} vehicle IDs.")

    for vid in vehicle_ids:
        print(f"Fetching data for vehicle ID: {vid}...")
        data = fetch_data(vid)
        if data:
            save_json(vid, data)

if __name__ == "__main__":
    main()
