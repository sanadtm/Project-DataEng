from google.cloud import pubsub_v1 
import time
import threading
import json
import pandas as pd
import psycopg2
from collections import defaultdict
import statistics
from datetime import datetime, timedelta

# Pub/Sub configuration
project_id = "dataengr-dataguru"
sub_id = "project-topic-sub"

# PostgreSQL connection
conn = psycopg2.connect(
    dbname="trimet",
    user="postgres",
    password="123456",
    host="localhost"
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

# Store previous message for inter-record checks
previous_data = {}

# Load reference data for validation
with open("2025-05-05.json", "r") as f:
    reference_data = pd.DataFrame(json.load(f))

# VALIDATION
def validate_message(data):
    global passed_count, failed_count
    errors = []

    # Existence
    if not data.get("VEHICLE_ID"):
        errors.append("Missing VEHICLE_ID")
    
    if not data.get("OPD_DATE"):
        errors.append("Missing OPD_DATE")
    
    if "GPS_LATITUDE" not in data or "GPS_LONGITUDE" not in data:
        errors.append("Missing GPS coordinates")

    # Limit

    # Limit assertion: OPD_DATE must be in 2025
    '''
    date_str = data.get("OPD_DATE")
    if date_str is not None:
        date_val = pd.to_datetime(date_str, errors='coerce')
        if pd.isna(date_val) or date_val.year != 2025:
            errors.append("OPD_DATE must be in the year 2025")
    '''

    hdop = data.get("GPS_HDOP")
    if hdop is not None and (hdop <= 0 or hdop > 10):
        errors.append("GPS_HDOP out of acceptable range (0-10)")

    sats = data.get("GPS_SATELLITES")
    if sats is not None and (sats < 0 or sats > 20):
        errors.append("GPS_SATELLITES out of range (0-20)")

    act_time = data.get("ACT_TIME")
    if act_time is not None and act_time > 86400:
        errors.append("ACT_TIME exceeds maximum seconds in a day")

    # Intra-record
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
    except:
        pass
    return 0.0

# def callback(msg):
#     global last_msg_time
#     with lock:
#         decoded = msg.data.decode("utf-8")
#         try:
#             data = json.loads(decoded)
#         except:
#             msg.ack()
#             return
# 
#         key = (data.get("VEHICLE_ID"), data.get("EVENT_NO_TRIP"))
#         if key in previous_data:
#             prev = previous_data[key]
#             speed = calculate_speed(data, prev)
#         else:
#             speed = 0.0
#         data["SPEED"] = speed
#         previous_data[key] = data
# 
#         validate_message(data)
#         records.append(data)
# 
#         msg.ack()
#         last_msg_time = time.time()

# MESSAGE HANDLER
def callback(msg):
    global last_msg_time
    with lock:
        decoded = msg.data.decode("utf-8")

        try:
            data = json.loads(decoded)
        except Exception as e:
            print("Invalid JSON:", e)
            msg.ack()
            return

        key = (data.get("VEHICLE_ID"), data.get("EVENT_NO_TRIP"))

        if key in previous_data:
            prev = previous_data[key]

            # Calculate speed using current and previous breadcrumb
            speed = calculate_speed(data, prev)

            # Filter unrealistic speed only if > 45 m/s
            if speed > 45:
                print(f"Skipping message with unrealistic speed: {speed:.2f} m/s")
                msg.ack()
                return

            # Assign same speed to both prev and current (first+second)
            prev["SPEED"] = speed
            data["SPEED"] = speed

            # Validate both
            validate_message(prev)
            validate_message(data)
            
            # Store both to DB
            store_to_db(prev)
            store_to_db(data)

            records.append(prev)
            records.append(data)

            # Append and write both (only if prev["SPEED"] wasn't already set)
            # try:
                # with open("testing.jsonl", "a") as f:
                    # if "SPEED" not in prev:  # Only write prev once
                        # f.write(json.dumps(prev) + "\n")
                        # records.append(prev)
                    # f.write(json.dumps(data) + "\n")
                    # records.append(data)
            # except Exception as e:
                # print("Failed to write to file:", e)

        else:
            # First breadcrumb — store for now (write later)
            previous_data[key] = data
            msg.ack()
            last_msg_time = time.time()
            return

        # Update last seen breadcrumb for this vehicle/trip
        previous_data[key] = data
        msg.ack()
        last_msg_time = time.time()

# DB Insertion
def store_to_db(data):
    try:
        opd_date_raw = data.get("OPD_DATE")  # e.g., "14DEC2022:00:00:00"
        act_time = data.get("ACT_TIME")      # e.g., 54466 (seconds since midnight)
        tstamp_str = None

        # Safely parse OPD_DATE (e.g., "14DEC2022:00:00:00")
        if opd_date_raw and act_time is not None:
            # Parse using datetime.strptime
            opd_date = datetime.strptime(opd_date_raw.split(":")[0], "%d%b%Y")  # e.g., 14DEC2022
            tstamp = opd_date + timedelta(seconds=int(act_time))
            tstamp_str = tstamp.strftime("%Y-%m-%d %H:%M:%S")  # final format

        trip_id = data.get("EVENT_NO_TRIP")
        vehicle_id = data.get("VEHICLE_ID")

        if trip_id and vehicle_id:
            cur.execute("""
                INSERT INTO trip (trip_id, vehicle_id)
                VALUES (%s, %s)
                ON CONFLICT (trip_id) DO NOTHING;
            """, (trip_id, vehicle_id))

        cur.execute("""
            INSERT INTO breadcrumb (tstamp, latitude, longitude, speed, trip_id)
            VALUES (%s, %s, %s, %s, %s);
        """, (
            tstamp_str,
            data.get("GPS_LATITUDE"),
            data.get("GPS_LONGITUDE"),
            data.get("SPEED"),
            trip_id
        ))

        conn.commit()
        print(f"Inserted trip_id {trip_id} at {tstamp_str}")

    except Exception as e:
        conn.rollback()
        print("DB insert failed:", e)

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

# Cleanup
cur.close()
conn.close()

# POST-RUN SUMMARY
end = time.time()

# Run summary + inter-record + statistical assertions
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

# Summary
for date, vids in date_vehicle_ids.items():
    if len(vids) < 1000:
        print(f"Too few records on {date}: {len(vids)}")
    
    if len(vids) > 5000:
        print(f"Too many vehicles on {date}: {len(vids)}")

# Statistical
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
