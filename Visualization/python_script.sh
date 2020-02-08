#!/bin/bash

#Generate the MapPlot.geojson file for visualization
python3 convert.py

#Generate eps files
python3 index_vis.py
echo "Heatmap Generated!"
python3 plot_neighborhood.py
echo "Plot of Neighborhood Generated!"
python3 plot_type.py
echo "Plot of Crime Type Generated!"