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

# Buffers for copy_from
trip_buffer = []
breadcrumb_buffer = []

# Load reference data
with open("2025-05-05.json", "r") as f:
    reference_data = pd.DataFrame(json.load(f))

# VALIDATION
def validate_message(data):
    global passed_count, failed_count
    errors = []

    if not data.get("VEHICLE_ID"):
        errors.append("Missing VEHICLE_ID")
    
    if not data.get("OPD_DATE"):
        errors.append("Missing OPD_DATE")
    
    if "GPS_LATITUDE" not in data or "GPS_LONGITUDE" not in data:
        errors.append("Missing GPS coordinates")

    hdop = data.get("GPS_HDOP")
    if hdop is not None and (hdop <= 0 or hdop > 10):
        errors.append("GPS_HDOP out of acceptable range (0-10)")

    sats = data.get("GPS_SATELLITES")
    if sats is not None and (sats < 0 or sats > 20):
        errors.append("GPS_SATELLITES out of range (0-20)")

    act_time = data.get("ACT_TIME")
    if act_time is not None and act_time > 86400:
        errors.append("ACT_TIME exceeds maximum seconds in a day")

    if data.get("ACT_TIME") == 0 and data.get("GPS_HDOP", 0) > 5:
        errors.append("GPS_HDOP too high when ACT_TIME is 0")
    
    if data.get("GPS_SATELLITES") == 0 and data.get("GPS_HDOP", 0) < 10:
        errors.append("GPS_HDOP too low when no satellites are visible")

    if errors:
        failed_count += 1
    else:
        passed_count += 1

# SPEED CALCULATION
def calculate_speed(current, previous):
    try:
        delta_distance = current["METERS"] - previous["METERS"]
        delta_time = current["ACT_TIME"] - previous["ACT_TIME"]
        if delta_time > 0:
            return delta_distance / delta_time
    except Exception as e:
        print("Speed calc error:", e)
    return 0.0

# FLUSH BUFFERS
def flush_buffers():
    try:
        if trip_buffer:
            trip_csv = io.StringIO()
            for row in trip_buffer:
                trip_csv.write(f"{row[0]},{row[1]}\n")
            trip_csv.seek(0)
            cur.copy_from(trip_csv, 'trip', sep=',', columns=('trip_id', 'vehicle_id'))

        if breadcrumb_buffer:
            breadcrumb_csv = io.StringIO()
            for row in breadcrumb_buffer:
                breadcrumb_csv.write(f"{row[0]},{row[1]},{row[2]},{row[3]},{row[4]}\n")
            breadcrumb_csv.seek(0)
            cur.copy_from(breadcrumb_csv, 'breadcrumb', sep=',', columns=('tstamp', 'latitude', 'longitude', 'speed', 'trip_id'))

        conn.commit()
        trip_buffer.clear()
        breadcrumb_buffer.clear()

    except Exception as e:
        conn.rollback()
        print("Bulk insert failed:", e)

# STORE TO DB
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
        print("DB buffer append failed:", e)

# CALLBACK
def callback(msg):
    global last_msg_time
    with lock:
        try:
            data = json.loads(msg.data.decode("utf-8"))
        except Exception as e:
            print("Invalid JSON:", e)
            msg.ack()
            return

        key = (data.get("VEHICLE_ID"), data.get("EVENT_NO_TRIP"))

        if key in previous_data:
            prev = previous_data[key]
            speed = calculate_speed(data, prev)

            if speed > 45:
                print(f"Skipping message with unrealistic speed: {speed:.2f} m/s")
                msg.ack()
                return

            prev["SPEED"] = speed
            data["SPEED"] = speed

            validate_message(prev)
            validate_message(data)

            store_to_db(prev)
            store_to_db(data)

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

# SUBSCRIBER
subscriber = pubsub_v1.SubscriberClient()
sub_path = subscriber.subscription_path(project_id, sub_id)
pull = subscriber.subscribe(sub_path, callback=callback)

start = time.time()
print("Starting Listening for Messages at", sub_path, "...\n")

try:
    while True:
        time.sleep(1)
        if time.time() - last_msg_time > idle_seconds:
            pull.cancel()
            break
except KeyboardInterrupt:
    pull.cancel()

pull.result()

# FINALIZE
flush_buffers()
cur.close()
conn.close()

# SUMMARY
end = time.time()
print("\n--- Post-run Assertions ---")
date_vehicle_ids = defaultdict(set)
vehicle_times = defaultdict(list)
act_times = []

for record in records:
    date = record.get("OPD_DATE")
    vid = record.get("VEHICLE_ID")
    act_time = record.get("ACT_TIME")

    if date and vid:
        date_vehicle_ids[date].add(vid)
    
    if vid and act_time is not None:
        vehicle_times[vid].append(act_time)
    
    if act_time is not None:
        act_times.append(act_time)

for date, vids in date_vehicle_ids.items():
    if len(vids) < 1000:
        print(f"Too few records on {date}: {len(vids)}")
    
    if len(vids) > 5000:
        print(f"Too many vehicles on {date}: {len(vids)}")

if act_times:
    median = statistics.median(act_times)
    if median < 20000 or median > 60000:
        print(f"Median ACT_TIME {median} is outside expected range")
    else:
        print(f"Median ACT_TIME {median} is within expected range")

print("\n--- Validation Summary ---")
print("Passed:", passed_count)
print("Failed:", failed_count)
print("Total:", passed_count + failed_count)
print("Total runtime:", round(end - start, 2), "seconds")
