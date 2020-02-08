package ucr.cs226.project;

import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.catalyst.encoders.RowEncoder;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

public class SafetyIndex{

    private static class Locations implements Serializable {
        Double latitude;
        Double longitude;
        Integer ZIPCode;
        String neighborhood;
        String borough;
        public Locations(String ZIP, String lat, String lon, String neighbor, String boro){
            latitude = Double.parseDouble(lat);
            longitude = Double.parseDouble(lon);
            ZIPCode = Integer.parseInt(ZIP);
            neighborhood = neighbor;
            borough = boro;
        }
    }

    private static List<Locations> readNYCLocations(String file){
        List<Locations> locations = new ArrayList<Locations>();

        try {
            BufferedReader br = new BufferedReader(new FileReader(file));
            String line = br.readLine();
            while(line != null){
                String[] token = line.split(",");
                if (token[0].equals("ZIPCode")){
                    line = br.readLine();
                    continue;
                }
                locations.add(new Locations(token[0], token[1], token[2], token[3], token[4].toUpperCase()));
                line = br.readLine();
            }
        } catch (IOException e) {
            e.printStackTrace();
        }

        return locations;
    }

    private static String[] setZIPNeighbor(Double latitude, Double longitude, String borough, List<Locations> statesList){
        double nearest = -1.0;
        String[] nearestLoc = new String[2];

        Iterator<Locations> iterator = statesList.iterator();
        while (iterator.hasNext()){
            Locations locations = iterator.next();
            if(!borough.equals(locations.borough)) continue;
            double dist = Math.sqrt((latitude - locations.latitude)*(latitude - locations.latitude) + (longitude - locations.longitude)*(longitude - locations.longitude));
            if(nearest < 0.0 || dist < nearest){
                nearest = dist;
                nearestLoc[0] = locations.ZIPCode.toString();
                nearestLoc[1] = locations.neighborhood;
            }
        }
        return nearestLoc;
    }

	/**
	*Process NYPD_Complaint_Data_Historic dataset,
	*use NYC_Location_Dataset and NYC_Crimes datasets
	*to get the clean dataset NYC_Crime_Dataset (the main dataset for the project)
	*/
    private static void createCrimeNYCDataset(SparkSession session, String filePath){
        String locationFile = filePath+"NYC_Location_Dataset.csv";
        final List<Locations> NYCLocations = readNYCLocations(locationFile);

        String crimeFile = filePath+"NYPD_Complaint_Data_Historic.csv";
        Dataset<Row> crimeLocations = session.read()
                .option("delimiter", ",")
                .option("header", "true")
                .option("inferSchema", "true")
                .csv(crimeFile);
        crimeLocations.createOrReplaceTempView("crimeNYCLocations");
        Dataset<Row> crimeFilteredData = session.sqlContext().sql("SELECT CMPLNT_NUM AS ID, KY_CD AS crimeID, OFNS_DESC AS crimeType, BORO_NM AS Borough, Latitude, Longitude "
                + "FROM crimeNYCLocations WHERE BORO_NM IS NOT NULL AND Latitude IS NOT NULL");

        List<StructField> fields = new ArrayList<StructField>();
        int counter = 0;
        String structNames = "ID crimeID crimeType Latitude Longitude ZIPCode Neighborhood Borough";
        for (String field: structNames.split(" ")){
            StructField fieldName;
            if(counter == 0 || counter == 1 || counter == 5)
                fieldName = DataTypes.createStructField(field, DataTypes.IntegerType, true);
            else if(counter == 3 || counter == 4)
                fieldName = DataTypes.createStructField(field, DataTypes.DoubleType, true);
            else
                fieldName = DataTypes.createStructField(field, DataTypes.StringType, true);
            fields.add(fieldName);
            counter++;
        }
        StructType schema = DataTypes.createStructType(fields);

        Dataset<Row> crimeLocationsFullData = crimeFilteredData.map(new MapFunction<Row, Row>() {
            public Row call(Row row) throws Exception {
                String[] nearestLoc = setZIPNeighbor(row.getDouble(4), row.getDouble(5), row.getString(3), NYCLocations);
                Integer ZIP = (nearestLoc[0] == null)? 0: Integer.parseInt(nearestLoc[0]);
                String neighbor = nearestLoc[1];
                return RowFactory.create(row.getInt(0), row.getInt(1), row.getString(2),
                        row.getDouble(4), row.getDouble(5), ZIP, neighbor, row.getString(3));
            }
        }, RowEncoder.apply(schema));
        crimeLocationsFullData.createOrReplaceTempView("crimeData");

        Dataset<Row> crimeIDRate = session.read()
                .option("header", "true")
                .option("delimiter", ",")
                .option("inferSchema", "true")
                .csv(filePath+"NYC_Crimes.csv");
        crimeIDRate.createOrReplaceTempView("crimeIDWithRates");
        Dataset<Row> crimeLocationsFullDataWithRates = session.sqlContext().sql("SELECT newID as crimeID, Latitude, Longitude, ZIPCode"
                //+ ", seriousness"
                + " FROM crimeData, crimeIDWithRates WHERE crimeData.crimeID = crimeIDWithRates.crimeID");
        crimeLocationsFullDataWithRates.write().option("header", "true").option("delimiter", ",").csv(filePath+"NYC_Crime_Dataset");
    }

	/**
	*Read and process the NYC_ZIP_Geometry dataset
	*/
    private static Dataset<Row> readZIPGeoLocation(SparkSession session, String filePath){
        Dataset<Row> geoFile = session.read()
                .option("delimiter", ",")
                .option("header", "true")
                .option("inferSchema", "true")
                .csv(filePath+"NYC_ZIP_Geometry.csv");
        geoFile.createOrReplaceTempView("geoData");
        Dataset<Row> selectGeoFile = session.sqlContext().sql("SELECT geometry, coordinates, postalcode FROM geoData");

        List<StructField> fields = new ArrayList<StructField>();
        StructField fieldName = DataTypes.createStructField("type", DataTypes.StringType, true);
        fields.add(fieldName);
        fieldName = DataTypes.createStructField("coordinates", DataTypes.StringType, true);
        fields.add(fieldName);
        fieldName = DataTypes.createStructField("ZIPCode", DataTypes.IntegerType, true);
        fields.add(fieldName);
        StructType schema = DataTypes.createStructType(fields);

        Dataset<Row> geoLocationData = selectGeoFile.map(new MapFunction<Row, Row>() {
            public Row call(Row row) throws Exception {
                String polygon = "[[[";
                String coord = row.getString(1);
                String[] token = coord.split(";");
                int count = 0;
                while(count < token.length){
                    polygon += "["+token[count] + ",";
                    count++;
                    polygon += token[count];
                    count++;
                    if(count == token.length) polygon += "]]]]";
                    else polygon += "],";
                }
                return RowFactory.create(row.getString(0), polygon, row.getInt(2));
            }
        }, RowEncoder.apply(schema));
        geoLocationData.createOrReplaceTempView("geoLocationData");
        return geoLocationData;
    }

    private static void calculateSafetyIndex(SparkSession session, String filePath){
        String locationFile = filePath+ "NYC_Location_Dataset.csv";
        Dataset<Row> zipLocations = session.read()
                .option("delimiter", ",")
                .option("header", "true")
                .option("inferSchema", "true")
                .csv(locationFile);
        zipLocations.createOrReplaceTempView("ZIPLocations");

        String crimeRateFile = filePath+ "NYC_Crimes.csv";
        Dataset<Row> crimeRates = session.read()
                .option("delimiter", ",")
                .option("header", "true")
                .option("inferSchema", "true")
                .csv(crimeRateFile);
        crimeRates.createOrReplaceTempView("crimeRates");

        String crimeFile = filePath+ "NYC_Crime_Dataset.csv";
        Dataset<Row> crimeLocations = session.read()
                .option("delimiter", ",")
                .option("header", "true")
                .option("inferSchema", "true")
                .csv(crimeFile);
        crimeLocations.createOrReplaceTempView("crimeNYCData");

        Dataset<Row> crimeCountTypeArea = session.sqlContext().sql("SELECT crimeID, COUNT(*) AS X_n_i, ZIPCode"
                + " FROM crimeNYCData GROUP BY crimeID, ZIPCode");
        crimeCountTypeArea.createOrReplaceTempView("crimeCountTypeArea");

        Dataset<Row> maxCrimeCountType = session.sqlContext().sql("SELECT crimeID, MAX(X_n_i) AS max_X_n FROM crimeCountTypeArea GROUP BY crimeID");
        maxCrimeCountType.createOrReplaceTempView("maxCrimeCountType");
        Dataset<Row> minCrimeCountType = session.sqlContext().sql("SELECT crimeID, MIN(X_n_i) AS min_X_n FROM crimeCountTypeArea GROUP BY crimeID");
        minCrimeCountType.createOrReplaceTempView("minCrimeCountType");

        Dataset<Row> crimeCombined = session.sqlContext().sql("SELECT CA.crimeID AS crimeID, Weight/10 AS Weight, X_n_i, max_X_n, min_X_n, ZIPCode "
                + "FROM crimeCountTypeArea CA, maxCrimeCountType maxC, minCrimeCountType minC, crimeRates CR "
                + "WHERE CA.crimeID = maxC.crimeID AND CA.crimeID = minC.crimeID AND CA.crimeID = CR.newID "
                + "ORDER BY ZIPCode, crimeID");
        crimeCombined.createOrReplaceTempView("crimeCombined");
        //crimeCombined.filter("ZIPCode=10004").show(100);

        Dataset<Row> safetyIndexArea = session.sqlContext().sql("SELECT ZIPCode,"
                + " (1 - SUM((X_n_i - min_X_n)/(max_X_n - min_X_n)*Weight)/COUNT(*))*100 AS Safety_Index_ZIPCode"
                + " FROM crimeCombined GROUP BY ZIPCode ORDER BY Safety_Index_ZIPCode DESC");
        Dataset<Row> safetyIndexAreaLoc = safetyIndexArea.join(zipLocations, "ZIPCode");

        Dataset<Row> readGeo = readZIPGeoLocation(session, filePath);
        Dataset<Row> safetyIndexGeoLoc = safetyIndexAreaLoc.join(readGeo, "ZIPCode");
        safetyIndexGeoLoc.coalesce(1).write().option("header", "true").option("delimiter", ",").csv(filePath+"Safety");

        /**
         * Calculate Statistics
         * 1. per crime type
         * 2. per neighborhood
         */
        Dataset<Row> statCrimeType = session.sqlContext().sql("SELECT newType as Crime_Type, COUNT(*) AS Crimes_Count"
                + " FROM crimeNYCData X, crimeRates Y WHERE X.crimeID = Y.newID GROUP BY Crime_Type ORDER BY Crimes_Count");
        statCrimeType.coalesce(1).write().option("header", "true").option("delimiter", ",").csv(filePath+ "Stat/CrimeType");

        Dataset<Row> statCrimeNeighborhood = session.sqlContext().sql("SELECT neighborhood AS Neighborhood, COUNT(*) AS Crimes_Count"
                + " FROM crimeNYCData X, crimeRates Y, ZIPLocations Z WHERE X.crimeID = Y.newID AND Z.ZIPCode = X.ZIPCode"
                + " GROUP BY neighborhood ORDER BY Crimes_Count");
        statCrimeNeighborhood.coalesce(1).write().option("header", "true").option("delimiter", ",").csv(filePath+"Stat/Neighborhood");
    }

    public static void main(String[] args){
        SparkSession session = SparkSession.builder().appName("CS226 Project").getOrCreate();
        JavaSparkContext context = new JavaSparkContext(session.sparkContext());

		/**
		*Call createCrimeNYCDataset() method if NYC_Crime_Dataset.csv does not exist
		*Uncomment the following statement and
		*Comment out the calculateSafetyIndex() method
		*/
        //createCrimeNYCDataset(session, args[0]);
		
		/**
		*Call calculateSafetyIndex() method to run the project
		*and submit the jobs to cluster nodes
		*N.B.: comment out the createCrimeNYCDataset() method before the next statement
		*/
        calculateSafetyIndex(session, args[0]);

        context.stop();
    }
}
