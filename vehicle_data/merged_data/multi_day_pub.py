import glob
import json
import time
from google.cloud import pubsub_v1

# Configuration
SERVICE_ACCOUNT_FILE = "/opt/dataengr-dataguru-809ccf8d3880.json"
PROJECT_ID = "dataengr-dataguru"
TOPIC_ID = "project-topic"

# Publisher setup
publisher = pubsub_v1.PublisherClient()
topic_path = publisher.topic_path(PROJECT_ID, TOPIC_ID)

def main():
    json_files = sorted(glob.glob("2025-??-??.json"))
    total_count = 0
    start = time.time()

    for filename in json_files:
        with open(filename, "r") as f:
            records = json.load(f)

        batch = []
        for i, record in enumerate(records):
            data = json.dumps(record).encode("utf-8")
            batch.append(data)

            # Send in batches of 5000 for performance
            if len(batch) == 5000:
                publish_batch(batch)
                batch = []

            total_count += 1
            if total_count % 50000 == 0:
                print(f"Published {total_count} messages...")

        # Publish any leftover messages
        if batch:
            publish_batch(batch)

        print(f"Done with file: {filename}")

    end = time.time()
    print(f" Published {total_count} messages to {topic_path}")
    print(f" Total runtime: {round(end - start, 2)} seconds")

def publish_batch(batch):
    futures = []
    for data in batch:
        future = publisher.publish(topic_path, data)
        futures.append(future)
    # Wait for just the last one to complete to avoid overwhelming memory
    if futures:
        futures[-1].result()

if __name__ == "__main__":
    main()

