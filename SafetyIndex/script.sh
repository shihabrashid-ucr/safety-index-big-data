#!/bin/sh

mvn package
spark-submit --class ucr.cs226.project.SafetyIndex target/SafetyIndex-1.0-SNAPSHOT.jar $1

head -1 NYC_Crime_Dataset/part-00000-*.csv > NYC_Crime_Dataset.csv
tail -n +2 -q NYC_Crime_Dataset/*.csv >> NYC_Crime_Dataset.csv
rm -Rf NYC_Crime_Dataset/
