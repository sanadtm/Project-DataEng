from urllib import request
import csv
import json
import os
import time
from datetime import datetime
from concurrent import futures
from google.oauth2 import service_account
from google.cloud import pubsub_v1

# ---------- CONFIG ----------
CSV_FILE = "Glitch Vehicle IDs - VehicleGroupsIDs.csv"
SERVICE_ACCOUNT_FILE = "/opt/dataengr-dataguru-809ccf8d3880.json"
PROJECT_ID = "dataengr-dataguru"
TOPIC_ID = "project-topic"
BASE_URL = "https://busdata.cs.pdx.edu/api/getBreadCrumbs?vehicle_id="

# Output file for today's records
DATE_STR = datetime.now().strftime("%Y-%m-%d")
COMBINED_FILE = f"{DATE_STR}.json"

# ---------- SETUP ----------
credentials = service_account.Credentials.from_service_account_file(SERVICE_ACCOUNT_FILE)
publisher = pubsub_v1.PublisherClient(credentials=credentials)
topic_path = publisher.topic_path(PROJECT_ID, TOPIC_ID)

# ---------- FUNCTIONS ----------
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
            print(f"[{vehicle_id}] Fetched {len(data)} records.")
            return data
    except Exception as e:
        print(f"[{vehicle_id}] Error fetching data: {e}")
    return []

def callback(future):
    try:
        future.result()
    except Exception as e:
        print(f"Unable to publish message: {e}")

# ---------- MAIN ----------
def main():
    vehicle_ids = get_vehicle_ids(CSV_FILE)
    print(f"Found {len(vehicle_ids)} vehicle IDs.")

    all_records = []
    start_gather = time.time()

    for vid in vehicle_ids:
        records = fetch_data(vid)
        all_records.extend(records)

    # Save all gathered records to a single file
    with open(COMBINED_FILE, "w") as f:
        json.dump(all_records, f, indent=2)
    print(f"Saved {len(all_records)} records to {COMBINED_FILE}")
    print(f"Data gathering took {round(time.time() - start_gather, 2)} seconds")

    # Publish records to Pub/Sub
    future_list = []
    start_publish = time.time()

    for i, record in enumerate(all_records):
        data_str = json.dumps(record)
        data = data_str.encode("utf-8")

        future = publisher.publish(topic_path, data)
        future.add_done_callback(callback)
        future_list.append(future)

        if (i + 1) % 50000 == 0:
            print(f"Published {i + 1} messages...")

    for future in futures.as_completed(future_list):
        continue

    print(f"Published {len(all_records)} messages to {topic_path}")
    print(f"Publishing took {round(time.time() - start_publish, 2)} seconds")

if __name__ == "__main__":
    main()

