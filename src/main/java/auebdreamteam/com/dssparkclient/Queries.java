package auebdreamteam.com.dssparkclient;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.streaming.StreamingQueryException;

import java.io.*;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.*;

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
    private static final String STOP_ID = "stopID";
    private static final String LINE_ID = "lineID";

    private Dataset<Row> df;

    private double midLongitude = -200;
    private double midLatitude = -200;

    private Dataset<Row> busesOfArea1 = null;
    private Dataset<Row> busesOfArea2 = null;
    private Dataset<Row> busesOfArea3 = null;
    private Dataset<Row> busesOfArea4 = null;

    private void calculateMids() {
        double minLongitude = (double) df.agg(min(LONG)).collectAsList().get(0).get(0);
        double maxLongitude = (double) df.agg(max(LONG)).collectAsList().get(0).get(0);
        double minLatitude = (double) df.agg(min(LAT)).collectAsList().get(0).get(0);
        double maxLatitude = (double) df.agg(max(LAT)).collectAsList().get(0).get(0);

        midLongitude = (minLongitude + maxLongitude) / 2;
        midLatitude = (minLatitude + maxLatitude) / 2;
    }

    private void calculateAreas() {
        busesOfArea1 = df.filter(df.col(LAT).gt(midLatitude).and(df.col(LONG).gt(midLongitude)));
        busesOfArea2 = df.filter(df.col(LAT).lt(midLatitude).and(df.col(LONG).lt(midLongitude)));
        busesOfArea3 = df.filter(df.col(LAT).gt(midLatitude).and(df.col(LONG).lt(midLongitude)));
        busesOfArea4 = df.filter(df.col(LAT).lt(midLatitude).and(df.col(LONG).gt(midLongitude)));
    }

    Queries(Dataset<Row> df){
        this.df=df;
    }

    // Query #1
    void busesPerArea(){

        if(midLatitude == -200) {
            calculateMids();
        }

        if(busesOfArea1 == null) {
            calculateAreas();
        }

        Dataset<Row> df1 = busesOfArea1.dropDuplicates(VEHICLE_JOURNEY_ID, DATE).sort(DATE).groupBy(DATE).count().withColumn("Area", lit("Area 1"));
        Dataset<Row> df2 = busesOfArea2.dropDuplicates(VEHICLE_JOURNEY_ID, DATE).sort(DATE).groupBy(DATE).count().withColumn("Area", lit("Area 2"));
        Dataset<Row> df3 = busesOfArea3.dropDuplicates(VEHICLE_JOURNEY_ID, DATE).sort(DATE).groupBy(DATE).count().withColumn("Area", lit("Area 3"));
        Dataset<Row> df4 = busesOfArea4.dropDuplicates(VEHICLE_JOURNEY_ID, DATE).sort(DATE).groupBy(DATE).count().withColumn("Area", lit("Area 4"));

        Dataset<Row> dfConcated = df1.union(df2).union(df3).union(df4).sort(DATE, "Area");

        dfConcated.show(150);
    }

    // Query #2
    public void congestedBuses(){

        if(midLatitude == -200) {
            calculateMids();
        }

        if(busesOfArea1 == null) {
            calculateAreas();
        }

        Dataset<Row> df1 = busesOfArea1.filter(col(DATE).lt("2013-01-16")).dropDuplicates(VEHICLE_JOURNEY_ID, DATE, HOUR).groupBy(DATE, HOUR).count().withColumn("Area", lit("Area 1"));
        Dataset<Row> df2 = busesOfArea2.filter(col(DATE).lt("2013-01-16")).dropDuplicates(VEHICLE_JOURNEY_ID, DATE, HOUR).groupBy(DATE, HOUR).count().withColumn("Area", lit("Area 2"));
        Dataset<Row> df3 = busesOfArea3.filter(col(DATE).lt("2013-01-16")).dropDuplicates(VEHICLE_JOURNEY_ID, DATE, HOUR).groupBy(DATE, HOUR).count().withColumn("Area", lit("Area 3"));
        Dataset<Row> df4 = busesOfArea4.filter(col(DATE).lt("2013-01-16")).dropDuplicates(VEHICLE_JOURNEY_ID, DATE, HOUR).groupBy(DATE, HOUR).count().withColumn("Area", lit("Area 4"));

        Dataset<Row> dfConcated = df1.union(df2).union(df3).union(df4);

        Dataset<Row> trained = dfConcated.select(avg("Count")); 
        trained.show();

        //New Datasets so that we calculate them only once
        Dataset<Row> dfBusesAfterDateArea1 = busesOfArea1.filter(col(DATE).geq("2013-01-16"));
        Dataset<Row> dfBusesAfterDateArea2 = busesOfArea2.filter(col(DATE).geq("2013-01-16"));
        Dataset<Row> dfBusesAfterDateArea3 = busesOfArea3.filter(col(DATE).geq("2013-01-16"));
        Dataset<Row> dfBusesAfterDateArea4 = busesOfArea4.filter(col(DATE).geq("2013-01-16"));


        double average_buses = (double) trained.collectAsList().get(0).get(0);

        System.out.println("AREA #1");
        if (dfBusesAfterDateArea1.count() > average_buses){
            dfBusesAfterDateArea1.filter(df.col(CONGESTION).equalTo(1)).groupBy(DATE,HOUR).count().show();
        }

        System.out.println("AREA #2");
        if (dfBusesAfterDateArea2.count() > average_buses){
            dfBusesAfterDateArea2.filter(df.col(CONGESTION).equalTo(1)).groupBy(DATE,HOUR).count().show();
        }

        System.out.println("AREA #3");
        if (dfBusesAfterDateArea3.count() > average_buses){
            dfBusesAfterDateArea3.filter(df.col(CONGESTION).equalTo(1)).groupBy(DATE,HOUR).count().show();
        }

        System.out.println("AREA #4");
        if (dfBusesAfterDateArea4.count() > average_buses){
            dfBusesAfterDateArea4.filter(df.col(CONGESTION).equalTo(1)).groupBy(DATE,HOUR).count().show();
        }


    }

    // Query #3
    private static Map<String, Set<Integer>> map;
    void stopsPerLine(){
        map = new HashMap<>();
        df.dropDuplicates(LINE_ID, STOP_ID, AT_STOP).filter(df.col(AT_STOP).equalTo(1)).foreach(row -> {
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
                .and(df.col(STOP_ID).equalTo(stopID))).groupBy(LINE_ID).count().sort(LINE_ID).show(50);
    }

    // Query #4 Streaming
    void busesAtStopStreaming(String date, int hour, int stopID){
//        df.filter(col(AT_STOP).equalTo(1)
//                .and(col(DATE).equalTo(date))
//                .and(col("Hour").equalTo(hour))
//                .and(col(STOP_ID).equalTo(stopID))).groupBy(LINE_ID).count()
                //.sort(LINE_ID)
        StreamingQuery query = df.writeStream().format("console").start();


        try {
            query.awaitTermination();
        } catch (StreamingQueryException e){
            e.printStackTrace();
        }
    }

    // Query #5
    // In batch processing "last hour" is not feasible, so we run the query for every hour of each day.
    void busesAtStopInAreaBatch(double minLatitude, double minLongitude, double maxLatitude, double maxLongitude){
        df.filter(df.col(AT_STOP).equalTo(1)
                .and(df.col(LAT).gt(minLatitude)).and(df.col(LONG).gt(minLongitude))
                .and(df.col(LAT).lt(maxLatitude)).and(df.col(LONG).lt(maxLongitude)))
                .dropDuplicates("vehicleJourneyID", STOP_ID) // Counting each stop during a journey only once.
                .groupBy(DATE, "Hour").count().sort(DATE,"Hour").show();
    }

    // Query #6
    void timeToStop(String lineID, String timeFrame, int stopID){
        Dataset<Row> tempInitial = df.filter(df.col(DATE).equalTo(timeFrame)
                .and(df.col(LINE_ID).equalTo(lineID)));

        Dataset<Row> matchingStops = tempInitial.filter(tempInitial.col(AT_STOP).equalTo(1)
                .and(tempInitial.col(STOP_ID).equalTo(stopID)))
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
