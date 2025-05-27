import csv
import json
import os
import time
from urllib import request, error
from bs4 import BeautifulSoup
from google.cloud import pubsub_v1
from google.cloud.pubsub_v1 import types

PROJECT_ID = "dataengr-dataguru"
TOPIC_ID = "stop-event-topic"
CSV_FILE = "Glitch Vehicle IDs - VehicleGroupsIDs.csv"
BASE_URL = "https://busdata.cs.pdx.edu/api/getStopEvents?vehicle_num="


# --- SCRAPER ---
class StopEventScraper:
    def __init__(self, base_url):
        self.base_url = base_url

    def fetch_vehicle_data(self, vehicle_num):
        url = f"{self.base_url}{vehicle_num}"
        try:
            with request.urlopen(url) as response:
                html = response.read().decode("utf-8")
                soup = BeautifulSoup(html, "html.parser")

                date_str = self._extract_date(soup)
                records = self._parse_tables(soup, date_str, vehicle_num)
                return records
        except Exception as e:
            print(f"[{vehicle_num}] Error fetching/parsing HTML: {e}")
            return []

    def _extract_date(self, soup):
        header = soup.find("h1")
        if header and "for" in header.text:
            parts = header.text.strip().split("for")
            return parts[1].strip() if len(parts) > 1 else "unknown_date"
        return "unknown_date"

    def _parse_tables(self, soup, date_str, vehicle_num):
        all_records = []
        for table in soup.find_all("table"):
            headers = [th.text.strip() for th in table.find_all("th")]
            for tr in table.find_all("tr")[1:]:
                cells = [td.text.strip() for td in tr.find_all("td")]
                if len(cells) == len(headers):
                    record = dict(zip(headers, cells))
                    record["scraped_date"] = date_str
                    record["vehicle_number"] = vehicle_num
                    all_records.append(record)
        return all_records


# --- PUBLISHER ---
class StopEventPublisher:
    def __init__(self, project_id, topic_id):
        batch_settings = types.BatchSettings(
            max_bytes=1024 * 1024 * 5,
            max_latency=1.0,
            max_messages=1000
        )
        self.publisher = pubsub_v1.PublisherClient(batch_settings=batch_settings)
        self.topic_path = self.publisher.topic_path(project_id, topic_id)

    def publish_records(self, records):
        count = 0
        futures = []

        for record in records:
            data_str = json.dumps(record)
            future = self.publisher.publish(self.topic_path, data=data_str.encode("utf-8"))
            futures.append(future)

        for i, future in enumerate(futures, 1):
            try:
                future.result()
            except Exception as e:
                print(f"Error publishing message {i}: {e}")
            if i % 50000 == 0:
                print(f"Published {i} messages...")

        print(f"Finished publishing {len(records)} messages.")


# --- OPERATING PUBLISHER ---
class StopEventPipeline:
    def __init__(self, csv_file, scraper, publisher):
        self.csv_file = csv_file
        self.scraper = scraper
        self.publisher = publisher

    def run(self):
        vehicle_ids = self._load_vehicle_ids()
        print(f"Found {len(vehicle_ids)} vehicle IDs.")

        for vid in vehicle_ids:
            print(f"Fetching stop events for vehicle {vid}...")
            records = self.scraper.fetch_vehicle_data(vid)
            if records:
                print(f"Publishing {len(records)} records for vehicle {vid}...")
                self.publisher.publish_records(records)

    def _load_vehicle_ids(self):
        vehicle_ids = set()
        with open(self.csv_file, newline='') as csvfile:
            reader = csv.reader(csvfile)
            for row in reader:
                for item in row:
                    item = item.strip()
                    if item.isdigit():
                        vehicle_ids.add(item)
        return list(vehicle_ids)


# --- MAIN ---
def main():
    scraper = StopEventScraper(BASE_URL)
    publisher = StopEventPublisher(PROJECT_ID, TOPIC_ID)
    pipeline = StopEventPipeline(CSV_FILE, scraper, publisher)
    pipeline.run()


if __name__ == "__main__":
    main()

