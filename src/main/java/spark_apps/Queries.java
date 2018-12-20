package spark_apps;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import static org.apache.spark.sql.functions.max;
import static org.apache.spark.sql.functions.min;

class Queries {

	private static final String DATE = "Date";
	private static final String LAT = "latitude";
	private static final String LONG = "longitude";
	private static final String VEHICLE_ID = "vehicleID";
	private static final String AT_STOP = "atStop";
    private Dataset<Row> df;

    Queries(Dataset<Row> df){
        this.df=df;
    }

    void busesPerArea(){

	    @SuppressWarnings("Duplicates")
        double minLongitude = (double) df.agg(min(LONG)).collectAsList().get(0).get(0);
        double maxLongitude = (double) df.agg(max(LONG)).collectAsList().get(0).get(0);
        double minLatitude = (double) df.agg(min(LAT)).collectAsList().get(0).get(0);
        double maxLatitude = (double) df.agg(max(LAT)).collectAsList().get(0).get(0);

        double midLongitude = (minLongitude + maxLongitude) / 2;
        double midLatitude = (minLatitude + maxLatitude) / 2;

        Dataset<Row> df1 = df.filter(df.col(LAT).gt(midLatitude).and(df.col(LONG).gt(midLongitude))).dropDuplicates(VEHICLE_ID, DATE).sort(DATE).groupBy(DATE).count();
        Dataset<Row> df2 = df.filter(df.col(LAT).lt(midLatitude).and(df.col(LONG).lt(midLongitude))).dropDuplicates(VEHICLE_ID, DATE).sort(DATE).groupBy(DATE).count();
        Dataset<Row> df3 = df.filter(df.col(LAT).gt(midLatitude).and(df.col(LONG).lt(midLongitude))).dropDuplicates(VEHICLE_ID, DATE).sort(DATE).groupBy(DATE).count();
        Dataset<Row> df4 = df.filter(df.col(LAT).lt(midLatitude).and(df.col(LONG).gt(midLongitude))).dropDuplicates(VEHICLE_ID, DATE).sort(DATE).groupBy(DATE).count();
    }

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
        //System.out.println(map.toString());
    }

    void busesAtStopBatch(){
        df.filter(df.col(AT_STOP).equalTo(1)).groupBy(DATE, "Hour", "stopID", "lineID").count().sort(DATE,"Hour").show(50);
    }

    void busesAtStopInAreaBatch(double minLatitude, double minLongitude, double maxLatitude, double maxLongitude){
        df.filter(df.col(AT_STOP).equalTo(1)
                .and(df.col(LAT).gt(minLatitude)).and(df.col(LONG).gt(minLongitude))
                .and(df.col(LAT).lt(maxLatitude)).and(df.col(LONG).lt(maxLongitude)))
                .groupBy(DATE, "Hour").count().sort(DATE,"Hour").show();
    }

    private static Map<Float, Long> tempTimes;
    private static long totalTime;
    void timeToStop(String lineID, String timeFrame, int stopID){
        tempTimes = new HashMap<>();
        totalTime = 0;
        Dataset<Row> tempInitial = df.filter(df.col(DATE).equalTo(timeFrame)
                .and(df.col("lineID").equalTo(lineID)));
        System.out.println("55: "+tempInitial.count());
        Dataset<Row> temp = tempInitial.filter(tempInitial.col(AT_STOP).equalTo(1)
                .and(tempInitial.col("stopID").equalTo(stopID)));

        System.out.println("1: "+temp.count());
        Dataset<Row> tempFirsts = tempInitial.sort("timestamp").dropDuplicates("vehicleJourneyID");
        System.out.println("1.5: "+tempFirsts.count());
        temp.foreach(row -> {
            tempTimes.put(row.getFloat(5), row.getLong(0));
        });
        System.out.println("2: "+tempTimes.size());

        tempFirsts.foreach(row -> {
            if(row.isNullAt(5) || row.size() < 5) {
                return;
            }
            if(tempTimes.containsKey(row.getFloat(5))){
                totalTime += tempTimes.get(row.getFloat(5)) - row.getLong(0);
            }
        });
        System.out.println("3");

        long averageTIme = totalTime/temp.count();
        System.out.println(String.format("Time to bus stop %d for line %s, at date %s: %d", stopID, lineID, timeFrame, averageTIme));
    }
}
