from urllib import request, error  
import csv
import os
import json
from bs4 import BeautifulSoup
from collections import defaultdict

# Constants
BASE_URL = "https://busdata.cs.pdx.edu/api/getStopEvents?vehicle_num="
CSV_FILE = "Glitch Vehicle IDs - VehicleGroupsIDs.csv"
OUTPUT_DIR = "stop_event_data"

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

def scrape_data(vehicle_num):
    url = f"{BASE_URL}{vehicle_num}"
    try:
        with request.urlopen(url) as response:
            html = response.read().decode("utf-8")
            soup = BeautifulSoup(html, "html.parser")

            # Try to extract the date from the header
            header = soup.find("h2")
            date_str = "unknown_date"
            if header and "for" in header.text:
                parts = header.text.strip().split("for")
                if len(parts) > 1:
                    date_str = parts[1].strip()

            # Parse table
            table = soup.find("table")
            if not table:
                return date_str, []

            headers = [th.text.strip() for th in table.find_all("th")]
            rows = []
            for tr in table.find_all("tr")[1:]:
                cells = [td.text.strip() for td in tr.find_all("td")]
                if len(cells) == len(headers):
                    rows.append(dict(zip(headers, cells)))

            return date_str, rows
    except Exception as e:
        print(f"[{vehicle_num}] Failed to fetch/parse HTML: {e}")
        return "error", []

def save_json(vehicle_num, date_str, records):
    filename = f"{OUTPUT_DIR}/{date_str}_{vehicle_num}.json"
    with open(filename, "w") as f:
        json.dump(records, f, indent=2)
    print(f"[{vehicle_num}] Saved {len(records)} records to {filename}")

def main():
    vehicle_ids = get_vehicle_ids(CSV_FILE)
    print(f"Found {len(vehicle_ids)} vehicle IDs.")

    for vid in vehicle_ids:
        print(f"Scraping stop event data for vehicle {vid}...")
        date_str, records = scrape_data(vid)
        if records:
            save_json(vid, date_str, records)

if __name__ == "__main__":
    main()

