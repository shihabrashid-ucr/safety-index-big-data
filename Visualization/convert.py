#!/usr/bin/env python3

import csv, json
from collections import OrderedDict

features = []
with open('Safety_Index.csv', newline='') as csvfile:
	reader = csv.reader(csvfile, delimiter=',')
	next(reader)
	for ZIPCode, Safety_Index_ZIPCode, Latitude, Longitude, Neighborhood, Borough, type, coordinates in reader:
		d = OrderedDict()
		d['type'] = 'Feature'
		d['geometry'] = {
			'type': type,
			'coordinates': coordinates
		}
		d['properties'] = {
			'ZIPCode': int(ZIPCode),
			'Safety_Index': float(Safety_Index_ZIPCode)
		}
		features.append(d)

d = OrderedDict()
d['type'] = 'FeatureCollection'
d['features'] = features
with open("MapPlot.geojson", "w") as f:
	f.write(json.dumps(d, sort_keys=False, indent=4))

with open("MapPlot.geojson") as in_json:
	newText = in_json.read().replace("\"[", "[")
with open("MapPlot.geojson", "w") as f:
	f.write(newText)
with open("MapPlot.geojson") as in_json:
	newText = in_json.read().replace("]\"", "]")
with open("MapPlot.geojson", "w") as f:
	f.write(newText)
