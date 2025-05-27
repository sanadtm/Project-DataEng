import os
import csv
import json
import time
from urllib.request import urlopen
from bs4 import BeautifulSoup
import re

CSV_FILE = "Glitch Vehicle IDs - VehicleGroupsIDs.csv"
BASE_URL = "https://busdata.cs.pdx.edu/api/getStopEvents?vehicle_num="
OUTPUT_DIR = "stop_event_data"

os.makedirs(OUTPUT_DIR, exist_ok=True)

def get_vehicle_nums(csv_file):
    vehicle_nums = set()
    with open(csv_file, newline='') as f:
        reader = csv.reader(f)
        for row in reader:
            for item in row:
                item = item.strip()
                if item.isdigit():
                    vehicle_nums.add(item)
    return list(vehicle_nums)

def fetch_html(vehicle_num):
    url = f"{BASE_URL}{vehicle_num}"
    try:
        with urlopen(url) as response:
            return response.read()
    except Exception as e:
        print(f"[{vehicle_num}] Error fetching data: {e}")
        return None

def parse_service_date(soup):
    heading = soup.find('h1')
    if heading:
        match = re.search(r"(\d{4}-\d{2}-\d{2})", heading.text)
        if match:
            return match.group(1)
    return time.strftime("%Y-%m-%d")

def parse_stop_events(html_data, vehicle_num):
    soup = BeautifulSoup(html_data, 'html.parser')
    service_date = parse_service_date(soup)

    data = []
    trip_sections = soup.find_all('h2', string=re.compile(r'Stop events for PDX_TRIP'))

    for h2 in trip_sections:
        trip_id_match = re.search(r'PDX_TRIP\s+(-?\d+)', h2.text)
        if not trip_id_match:
            continue
        trip_id = trip_id_match.group(1)

        table = h2.find_next('table')
        if not table:
            continue

        rows = table.find_all('tr')
        if not rows:
            continue

        headers = [th.text.strip() for th in rows[0].find_all('th')]

        for row in rows[1:]:
            cols = row.find_all('td')
            values = [td.text.strip() for td in cols]
            if len(values) != len(headers):
                continue
            record = dict(zip(headers, values))
            record['vehicle_num'] = vehicle_num
            record['trip_id'] = trip_id
            data.append(record)

    return service_date, data

def main():
    vehicle_nums = get_vehicle_nums(CSV_FILE)
    all_records = []
    service_date = None

    for num in vehicle_nums:
        print(f"Processing vehicle {num}...")
        html = fetch_html(num)
        if not html:
            continue
        current_date, records = parse_stop_events(html, num)
        if records:
            service_date = current_date  # Use the last valid one
            all_records.extend(records)
        time.sleep(0.2)

    if service_date and all_records:
        filename = os.path.join(OUTPUT_DIR, f"{service_date}.json")
        with open(filename, 'w') as f:
            json.dump(all_records, f, indent=2)
        print(f"Saved {len(all_records)} records to {filename}")
    else:
        print("No valid data to save.")

if __name__ == "__main__":
    main()

