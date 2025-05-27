#!/usr/bin/env python3
import csv
import json
from geojson import Feature, FeatureCollection, Point

features = []

# Input and output file paths
# Visual 1
#input_file = '/opt/Project-DataEng/visualization/tunnel_trip.tsv'
#output_file = '/opt/Project-DataEng/visualization/tunnel_trip.geojson'

# Visual 2
# input_file = "/opt/Project-DataEng/visualization/route20_trip.tsv"
# output_file = "/opt/Project-DataEng/visualization/route20_trip.geojson"

# Visual 3
# input_file = "/opt/Project-DataEng/visualization/psu_trip.tsv"
# output_file = "/opt/Project-DataEng/visualization/psu_trip.geojson"

# Visual 4
#input_file = "/opt/Project-DataEng/visualization/ladd_trip.tsv"
#output_file = "/opt/Project-DataEng/visualization/ladd_trip.geojson"

# Visual 5a: Longest trip
# input_file = "/opt/Project-DataEng/visualization/longest_trip.tsv"
# output_file = "/opt/Project-DataEng/visualization/longest_trip.geojson"

# Visual 5b: Fastest trip
# input_file = "/opt/Project-DataEng/visualization/fast_trip.tsv"
# output_file = "/opt/Project-DataEng/visualization/fast_trip.geojson"

# Visual 5c: Early trip
input_file = "/opt/Project-DataEng/visualization/early_trip.tsv"
output_file = "/opt/Project-DataEng/visualization/early_trip.geojson"

# Open the TSV file and read its rows
with open(input_file, newline='') as csvfile:
    reader = csv.reader(csvfile, delimiter='\t')
    next(reader)  # Skip the header line: longitude, latitude, speed

    for row in reader:
        if len(row) != 3:
            continue  # Skip rows that don't have exactly 3 columns

        try:
            lon = float(row[0].strip())
            lat = float(row[1].strip())
            speed = float(row[2].strip())

            feature = Feature(
                geometry=Point((lon, lat)),
                properties={'speed': speed}
            )
            features.append(feature)
        except ValueError:
            continue  # Skip rows with non-numeric values

# Create GeoJSON FeatureCollection and write to file
collection = FeatureCollection(features)

with open(output_file, 'w') as f:
    json.dump(collection, f, indent=2)

print(f"GeoJSON created: {output_file} with {len(features)} features.")

