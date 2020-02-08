#!/bin/sh

#Delete "Safety" directory, if exists
if [ -d Safety/ ]; then
rm -Rf Safety/;
fi

#Delete "Stat" directory, if exists
if [ -d Stat/ ]; then
rm -Rf Stat/;
fi

#setup environment
export HADOOP_CONF_DIR=$HADOOP_HOME/etc/hadoop
export YARN_CONF_DIR=$HADOOP_HOME/etc/hadoop

#Compile and execute the project on Spark
#$1 = Input files directory
mvn package
spark-submit --class ucr.cs226.project.SafetyIndex target/SafetyIndex-1.0-SNAPSHOT.jar $1


#Merge the partitioned output in one file
head -1 Safety/part-00000-*.csv > Safety_Index.csv
tail -n +2 -q Safety/*.csv >> Safety_Index.csv

#Merge the partitioned output of Statistics in one file
head -1 Stat/Neighborhood/part-00000-*.csv > Statistics_Neighborhood.csv
tail -n +2 -q Stat/Neighborhood/*.csv >> Statistics_Neighborhood.csv

head -1 Stat/CrimeType/part-00000-*.csv > Statistics_Crime_Type.csv
tail -n +2 -q Stat/CrimeType/*.csv >> Statistics_Crime_Type.csv

rm -Rf Stat/
rm -Rf Safety/
