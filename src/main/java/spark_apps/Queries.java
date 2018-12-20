package spark_apps;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import static org.apache.spark.sql.functions.*;


class Queries {

	private static final String DATE = "Date";
	private static final String LAT = "latitude";
	private static final String LONG = "longitude";
	private static final String VEHICLE_ID = "vehicleID";
	private static final String VEHICLE_JOURNEY_ID = "vehicleJourneyID";
	private static final String AT_STOP = "atStop";
    private static final String CONGESTION = "congestion";
    private static final String HOUR = "Hour";

    private Dataset<Row> df;

    Queries(Dataset<Row> df){
        this.df=df;
    }
    // Query #1
    void busesPerArea(){

	    @SuppressWarnings("Duplicates")
        double minLongitude = (double) df.agg(min(LONG)).collectAsList().get(0).get(0);
        double maxLongitude = (double) df.agg(max(LONG)).collectAsList().get(0).get(0);
        double minLatitude = (double) df.agg(min(LAT)).collectAsList().get(0).get(0);
        double maxLatitude = (double) df.agg(max(LAT)).collectAsList().get(0).get(0);

        double midLongitude = (minLongitude + maxLongitude) / 2;
        double midLatitude = (minLatitude + maxLatitude) / 2;

        Dataset<Row> df1 = df.filter(df.col(LAT).gt(midLatitude).and(df.col(LONG).gt(midLongitude))).dropDuplicates(VEHICLE_JOURNEY_ID, DATE).sort(DATE).groupBy(DATE).count().withColumn("Area", lit("Area 1"));
        Dataset<Row> df2 = df.filter(df.col(LAT).lt(midLatitude).and(df.col(LONG).lt(midLongitude))).dropDuplicates(VEHICLE_JOURNEY_ID, DATE).sort(DATE).groupBy(DATE).count().withColumn("Area", lit("Area 2"));
        Dataset<Row> df3 = df.filter(df.col(LAT).gt(midLatitude).and(df.col(LONG).lt(midLongitude))).dropDuplicates(VEHICLE_JOURNEY_ID, DATE).sort(DATE).groupBy(DATE).count().withColumn("Area", lit("Area 3"));
        Dataset<Row> df4 = df.filter(df.col(LAT).lt(midLatitude).and(df.col(LONG).gt(midLongitude))).dropDuplicates(VEHICLE_JOURNEY_ID, DATE).sort(DATE).groupBy(DATE).count().withColumn("Area", lit("Area 4"));

        Dataset<Row> df_concated = df1.union(df2).union(df3).union(df4).sort("Date", "Area");

        df_concated.show(150);



    }

    // Query #2
    public void congestedBuses(){

        double minLongitude = (double) df.agg(min(LONG)).collectAsList().get(0).get(0);
        double maxLongitude = (double) df.agg(max(LONG)).collectAsList().get(0).get(0);
        double minLatitude = (double) df.agg(min(LAT)).collectAsList().get(0).get(0);
        double maxLatitude = (double) df.agg(max(LAT)).collectAsList().get(0).get(0);

        double midLongitude = (minLongitude + maxLongitude) / 2;
        double midLatitude = (minLatitude + maxLatitude) / 2;


        //Average calculation(Area 1)-Number
        /*Dates in "" are just placeholder*/
        int y=(int)df.filter(df.col(LAT).gt(midLatitude).and(df.col(LONG).gt(midLongitude)).and(df.col(DATE).geq("2013-01-01")).and(df.col(DATE).leq("2013-01-15"))/*.and(df.col(DATE).leq(2013-01-15))*/).dropDuplicates(VEHICLE_JOURNEY_ID, DATE).sort(DATE).count();
        System.out.println(y);

        int x=(int)df.filter(df.col(LAT).lt(midLatitude).and(df.col(LONG).lt(midLongitude))).dropDuplicates(HOUR, DATE).sort(DATE).count();
        System.out.println(x);

        float avgBusesPerHour = y/x;
        System.out.println(avgBusesPerHour);

    }

    // Query #3
    private static Map<String, Set<Integer>> map;
    void stopsPerLine(){
        map = new HashMap<>();
        df.dropDuplicates("lineID", "stopID", AT_STOP).filter(df.col(AT_STOP).equalTo(1)).foreach(row -> {
            if(map.containsKey(row.getString(1))){
                Set<Integer> tempSet = map.get(row.getString(1));
                tempSet.add(row.getInt(13));
                map.put(row.getString(1), tempSet);
            } else {
                Set<Integer> tempSet = new HashSet<>(1);
                tempSet.add(row.getInt(13));
                map.put(row.getString(1), tempSet);
            }
        });


        for (Map.Entry<String, Set<Integer>> stringSetEntry : map.entrySet()) {
            System.out.println(String.format("Bus Line %s has Stops -> %s", stringSetEntry.getKey(), stringSetEntry.getValue().toString()));
        }
    }
    // Query #4
    void busesAtStopBatch(String date, int hour, int stopID){
        df.filter(df.col(AT_STOP).equalTo(1)
                .and(df.col(DATE).equalTo(date))
                .and(df.col("Hour").equalTo(hour))
                .and(df.col("stopID").equalTo(stopID))).groupBy("lineID").count().sort("lineID").show(50);
    }
    // Query #5
    // In batch processing "last hour" is not feasible, so we run the query for every hour of each day.
    void busesAtStopInAreaBatch(double minLatitude, double minLongitude, double maxLatitude, double maxLongitude){
        df.filter(df.col(AT_STOP).equalTo(1)
                .and(df.col(LAT).gt(minLatitude)).and(df.col(LONG).gt(minLongitude))
                .and(df.col(LAT).lt(maxLatitude)).and(df.col(LONG).lt(maxLongitude)))
                .dropDuplicates("vehicleJourneyID", "stopID") // Counting each stop during a journey only once.
                .groupBy(DATE, "Hour").count().sort(DATE,"Hour").show();
    }

    // Query #6
    void timeToStop(String lineID, String timeFrame, int stopID){
        Dataset<Row> tempInitial = df.filter(df.col(DATE).equalTo(timeFrame)
                .and(df.col("lineID").equalTo(lineID)));

        Dataset<Row> matchingStops = tempInitial.filter(tempInitial.col(AT_STOP).equalTo(1)
                .and(tempInitial.col("stopID").equalTo(stopID)))
                .sort("timestamp").dropDuplicates("vehicleJourneyID");


        Dataset<Row> tempFirsts = tempInitial.sort("timestamp").dropDuplicates("vehicleJourneyID")
                .select("timestamp", "vehicleJourneyID")
                .withColumnRenamed("timestamp", "startTime");

        Dataset<Row> average_time = matchingStops.join(tempFirsts, "vehicleJourneyID")
                .withColumn("diffTime", col("timestamp").divide(1000000)
                .minus(col("startTime")
                .divide(1000000))
                .divide(60));

        double averageTIme = average_time.agg(avg(average_time.col("diffTime"))).collectAsList().get(0).getDouble(0);

        System.out.println(String.format("Time to bus stop %d for line %s, at date %s: %f minutes", stopID, lineID, timeFrame, averageTIme));
    }
}
