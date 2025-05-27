from datetime import datetime
from google.cloud import pubsub_v1
import json
import time
import threading
import pandas as pd
import io
import psycopg2

# ========== Configurations ==========
PROJECT_ID = "dataengr-dataguru"
SUBSCRIPTION_ID = "stop-event-sub"
IDLE_SECONDS = 30
TABLE_NAME = "stop_data"

# ========== Database Handler ==========
class Database:
    def __init__(self):
        self.conn = psycopg2.connect(
            dbname="trimet",
            user="postgres",
            password="123456",
            host="localhost"
        )
        self.cur = self.conn.cursor()

    def copy_records(self, batch):
        if not batch:
            return
        buf = io.StringIO()
        for r in batch:
            row = [str(r[k]) for k in StopEventValidator.REQUIRED_FIELDS]
            buf.write("\t".join(row) + "\n")
        buf.seek(0)
        try:
            self.cur.copy_from(buf, TABLE_NAME, sep="\t", null="")
            self.conn.commit()
        except Exception as e:
            self.conn.rollback()
            print("COPY failed:", e)

    def close(self):
        self.cur.close()
        self.conn.close()

# ========== Validator ==========
class StopEventValidator:
    REQUIRED_FIELDS = [
        "vehicle_number", "leave_time", "train", "route_number", "direction", "service_key",
        "trip_number", "stop_time", "arrive_time", "dwell", "location_id", "door", "lift",
        "ons", "offs", "estimated_load", "maximum_speed", "train_mileage", "pattern_distance",
        "location_distance", "x_coordinate", "y_coordinate", "data_source", "schedule_status"
    ]

    def __init__(self):
        self.passed = 0
        self.failed = 0

    def validate(self, data):
        for field in self.REQUIRED_FIELDS:
            if field not in data or data[field] in ["", None]:
                self.failed += 1
                return False

        try:
            if not (0 <= int(data["leave_time"]) <= 86400):
                self.failed += 1
                return False
            if not (0 <= int(data["arrive_time"]) <= 86400):
                self.failed += 1
                return False
            if not (0 < float(data["maximum_speed"]) <= 100):
                self.failed += 1
                return False
            if float(data["x_coordinate"]) == 0 or float(data["y_coordinate"]) == 0:
                self.failed += 1
                return False
        except:
            self.failed += 1
            return False

        self.passed += 1
        return True

# ========== Subscriber ==========
class StopEventSubscriber:
    def __init__(self):
        self.records = []
        self.lock = threading.Lock()
        self.last_msg_time = time.time()
        self.db = Database()
        self.validator = StopEventValidator()

    def callback(self, msg):
        with self.lock:
            try:
                data = json.loads(msg.data.decode("utf-8"))
                if self.validator.validate(data):
                    self.records.append(data)
            except Exception as e:
                print("Error parsing message:", e)
            msg.ack()
            self.last_msg_time = time.time()

    def listen(self):
        subscriber = pubsub_v1.SubscriberClient()
        sub_path = subscriber.subscription_path(PROJECT_ID, SUBSCRIPTION_ID)
        stream = subscriber.subscribe(sub_path, callback=self.callback)

        print(f"Listening to {sub_path}...")

        try:
            while True:
                time.sleep(1)
                if time.time() - self.last_msg_time > IDLE_SECONDS:
                    print(f"\n⏳ No messages in {IDLE_SECONDS} seconds. Stopping...\n")
                    stream.cancel()
                    break
        except Exception as e:
            print("Error:", e)
            stream.cancel()

        stream.result()

        self.db.copy_records(self.records)
        self.print_summary()
        self.db.close()

    def print_summary(self):
        print("\n--- Summary ---")
        print(f"Total received: {self.validator.passed + self.validator.failed}")
        print(f"Passed: {self.validator.passed}")
        print(f"Failed: {self.validator.failed}")
        print(f"Stored: {len(self.records)}")

# ========== Run ==========
if __name__ == "__main__":
    StopEventSubscriber().listen()

