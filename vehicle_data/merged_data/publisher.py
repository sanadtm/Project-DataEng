import os
import json
import time
from concurrent import futures
from google.oauth2 import service_account
from google.cloud import pubsub_v1
from datetime import datetime

# Directories
SERVICE_ACCOUNT_FILE = "/opt/dataengr-dataguru-809ccf8d3880.json"
PROJECT_ID = "dataengr-dataguru"
TOPIC_ID = "project-topic"
#INPUT_FILE = "2025-04-19.json"
INPUT_FILE = datetime.now().strftime("%Y-%m-%d") + ".json"

# Publisher Config 
credentials = service_account.Credentials.from_service_account_file(SERVICE_ACCOUNT_FILE)
publisher = pubsub_v1.PublisherClient(credentials=credentials)
topic_path = publisher.topic_path(PROJECT_ID, TOPIC_ID)

def callback(future):
    try:
        future.result()
    except Exception as e:
        print(f"Unable to publish message: {e}")

# Publisher Function
def main():
    with open(INPUT_FILE, "r") as f:
        records = json.load(f)

    future_list = []
    count = 0
    start = time.time()

    for record in records:
        data_str = json.dumps(record)
        data = data_str.encode("utf-8")

        future = publisher.publish(topic_path, data)
        future.add_done_callback(callback)
        future_list.append(future)

        count += 1
        if count % 50000 == 0:
            print(f"Published {count} messages...")

    for future in futures.as_completed(future_list):
        continue

    end = time.time()
    print(f"Published {len(records)} messages to {topic_path}")
    print(f"Total runtime: {round(end - start, 2)} seconds")

if __name__ == "__main__":
    main()

