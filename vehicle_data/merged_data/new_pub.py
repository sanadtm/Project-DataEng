from google.cloud import pubsub_v1
import json
import time
from datetime import datetime

# Configuration
SERVICE_ACCOUNT_FILE = "/opt/dataengr-dataguru-809ccf8d3880.json"
PROJECT_ID = "dataengr-dataguru"
TOPIC_ID = "project-topic"
INPUT_FILE = "2025-05-07.json"  # assuming you're running from inside merged_data


#INPUT_FILE = sorted(glob.glob("2025-??-??.json"))
# Publisher setup
publisher = pubsub_v1.PublisherClient()
topic_path = publisher.topic_path(PROJECT_ID, TOPIC_ID)

def callback(future):
    try:
        future.result()
    except Exception as e:
        print(f"Publish failed: {e}")

def main():
    with open(INPUT_FILE, "r") as f:
        records = json.load(f)

    future_list = []
    start = time.time()
    for i, record in enumerate(records):
        data_str = json.dumps(record)
        data = data_str.encode("utf-8")
        future = publisher.publish(topic_path, data)
        future.add_done_callback(callback)
        future_list.append(future)

        if i % 5000 == 0:
            print(f"Published {i} messages...")

    for future in future_list:
        future.result()

    end = time.time()
    print(f"Published {len(records)} messages to {topic_path}")
    print(f"Total runtime: {round(end - start, 2)} seconds")

if __name__ == "__main__":
    main()

