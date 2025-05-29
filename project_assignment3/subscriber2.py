from google.cloud import pubsub_v1 
import time
import threading
import json
import pandas as pd
import psycopg2
import io
from collections import defaultdict
import statistics
from datetime import datetime, timedelta
import os

# Toggle this to True to enable debug prints
DEBUG = False

# Pub/Sub configuration
project_id = "dataengr-dataguru"
sub_id = "project-topic-sub"

# PostgreSQL connection using environment variables
conn = psycopg2.connect(
    dbname=os.getenv("DB_NAME", "trimet"),
    user=os.getenv("DB_USER", "postgres"),
    password=os.getenv("DB_PASSWORD", "123456"),
    host=os.getenv("DB_HOST", "localhost")
)
cur = conn.cursor()

# Shared state
msgs = []
records = []
lock = threading.Lock()
idle_seconds = 10

last_msg_time = time.time()
passed_count = 0
failed_count = 0
previous_data = {}

trip_buffer = []
breadcrumb_buffer = []

# Load File
with open("2025-05-05.json", "r") as f:
    reference_data = pd.DataFrame(json.load(f))

# Assertions Validations
def assert_vehicle_id(data): assert data.get("VEHICLE_ID") is not None
def assert_opd_date(data): assert data.get("OPD_DATE") is not None
def assert_gps_coordinates(data): assert "GPS_LATITUDE" in data and "GPS_LONGITUDE" in data
def assert_hdop_range(data):
    hdop = data.get("GPS_HDOP")
    if hdop is not None:
        assert 0 < hdop <= 10
def assert_sats_range(data):
    sats = data.get("GPS_SATELLITES")
    if sats is not None:
        assert 0 <= sats <= 20
def assert_act_time(data):
    act_time = data.get("ACT_TIME")
    if act_time is not None:
        assert act_time <= 86400
def assert_intra_hdop(data):
    if data.get("ACT_TIME") == 0 and data.get("GPS_HDOP", 0) > 5:
        raise AssertionError
def assert_intra_sats(data):
    if data.get("GPS_SATELLITES") == 0 and data.get("GPS_HDOP", 0) < 10:
        raise AssertionError

def validate_message(data):
    global passed_count, failed_count
    try:
        assert_vehicle_id(data)
        assert_opd_date(data)
        assert_gps_coordinates(data)
        assert_hdop_range(data)
        assert_sats_range(data)
        assert_act_time(data)
        assert_intra_hdop(data)
        assert_intra_sats(data)
        passed_count += 1
        if passed_count % 100000 == 0:
            print(f"Processed {passed_count} valid records...")
    except AssertionError:
        failed_count += 1
        if DEBUG:
            print("Validation failed:", data)

# Cal Speed
def calculate_speed(current, previous):
    try:
        delta_distance = current["METERS"] - previous["METERS"]
        delta_time = current["ACT_TIME"] - previous["ACT_TIME"]
        if delta_time > 0:
            return delta_distance / delta_time
    except Exception as e:
        if DEBUG:
            print("Speed calc error:", e)
    return 0.0

# Clean Buffers
def flush_buffers():
    global conn, cur
    try:
        if conn.closed:
            if DEBUG:
                print("Reconnecting to database...")
            conn = psycopg2.connect(
                dbname=os.getenv("DB_NAME", "trimet"),
                user=os.getenv("DB_USER", "postgres"),
                password=os.getenv("DB_PASSWORD", "123456"),
                host=os.getenv("DB_HOST", "localhost")
            )
            cur = conn.cursor()

        cur.execute("SELECT trip_id FROM trip;")
        existing_trip_ids = set(row[0] for row in cur.fetchall())

        seen_trip_ids = set()
        deduped_trip_buffer = []
        for row in trip_buffer:
            trip_id = row[0]
            if trip_id not in existing_trip_ids and trip_id not in seen_trip_ids:
                deduped_trip_buffer.append(row)
                seen_trip_ids.add(trip_id)

        if deduped_trip_buffer:
            trip_csv = io.StringIO()
            for row in deduped_trip_buffer:
                safe_row = [str(row[0]) if row[0] is not None else '',
                            str(row[1]) if row[1] is not None else '']
                trip_csv.write(','.join(safe_row) + '\n')
            trip_csv.seek(0)
            cur.copy_from(trip_csv, 'trip', sep=',', columns=('trip_id', 'vehicle_id'))

        if breadcrumb_buffer:
            breadcrumb_csv = io.StringIO()
            for row in breadcrumb_buffer:
                if None in row:
                    # if DEBUG: print("Skipping invalid breadcrumb row:", row)
                    continue
                safe_row = [
                    str(row[0]) if row[0] is not None else '',
                    str(row[1]) if row[1] is not None else '',
                    str(row[2]) if row[2] is not None else '',
                    str(row[3]) if row[3] is not None else '',
                    str(row[4]) if row[4] is not None else ''
                ]
                breadcrumb_csv.write(','.join(safe_row) + '\n')
            breadcrumb_csv.seek(0)
            cur.copy_from(breadcrumb_csv, 'breadcrumb', sep=',', columns=('tstamp', 'latitude', 'longitude', 'speed', 'trip_id'))

        conn.commit()
        trip_buffer.clear()
        breadcrumb_buffer.clear()

    except Exception as e:
        conn.rollback()
        print("Flush buffer failed:", e)

# Store to PostGres DB
def store_to_db(data):
    try:
        opd_date_raw = data.get("OPD_DATE")
        act_time = data.get("ACT_TIME")
        tstamp_str = None
        if opd_date_raw and act_time is not None:
            opd_date = datetime.strptime(opd_date_raw.split(":")[0], "%d%b%Y")
            tstamp = opd_date + timedelta(seconds=int(act_time))
            tstamp_str = tstamp.strftime("%Y-%m-%d %H:%M:%S")

        trip_id = data.get("EVENT_NO_TRIP")
        vehicle_id = data.get("VEHICLE_ID")
        if trip_id and vehicle_id:
            trip_buffer.append((trip_id, vehicle_id))

        breadcrumb_buffer.append((
            tstamp_str,
            data.get("GPS_LATITUDE"),
            data.get("GPS_LONGITUDE"),
            data.get("SPEED"),
            trip_id
        ))

        if len(breadcrumb_buffer) >= 500:
            flush_buffers()
    except Exception as e:
        if DEBUG:
            print("DB buffer append failed:", e)

# WRITE TO .jsonl
def save_to_jsonl(data):
    try:
        with open("transformed_output.jsonl", "a") as f:
            f.write(json.dumps(data) + "\n")
    except Exception as e:
        if DEBUG:
            print("Write to jsonl failed:", e)

def callback(msg):
    global last_msg_time
    with lock:
        try:
            data = json.loads(msg.data.decode("utf-8"))
        except Exception as e:
            if DEBUG:
                print("Invalid JSON:", e)
            msg.ack()
            return

        key = (data.get("VEHICLE_ID"), data.get("EVENT_NO_TRIP"))

        if key in previous_data:
            prev = previous_data[key]
            speed = calculate_speed(data, prev)

            if speed > 45:
                # if DEBUG: print(f"Skipping speed: {speed:.2f}")
                msg.ack()
                return

            prev["SPEED"] = speed
            data["SPEED"] = speed

            validate_message(prev)
            validate_message(data)

            store_to_db(prev)
            store_to_db(data)

            save_to_jsonl(prev)
            save_to_jsonl(data)

            records.append(prev)
            records.append(data)

        else:
            previous_data[key] = data
            msg.ack()
            last_msg_time = time.time()
            return

        previous_data[key] = data
        msg.ack()
        last_msg_time = time.time()

# Subscriber
subscriber = pubsub_v1.SubscriberClient()
sub_path = subscriber.subscription_path(project_id, sub_id)
pull = subscriber.subscribe(sub_path, callback=callback)

start = time.time()
print("Listening for Messages at", sub_path, "...")

try:
    while True:
        time.sleep(1)
        if time.time() - last_msg_time > idle_seconds:
            pull.cancel()
            break
except KeyboardInterrupt:
    pull.cancel()

try:
    pull.result()
except Exception as e:
    print("Subscriber shutdown exception (ignored):", e)
flush_buffers()
cur.close()
conn.close()

# Output
end = time.time()
print("\n--- Validation Summary ---")
print("Passed:", passed_count)
print("Failed:", failed_count)
print("Total:", passed_count + failed_count)
print("Total runtime:", round(end - start, 2), "seconds")

