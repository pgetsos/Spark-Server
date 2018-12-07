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

	private static final String TIMEFRAME = "timeFrame";
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

        Dataset<Row> df1 = df.filter(df.col(LAT).gt(midLatitude).and(df.col(LONG).gt(midLongitude))).dropDuplicates(VEHICLE_ID, TIMEFRAME).sort(TIMEFRAME).groupBy(TIMEFRAME).count();
        Dataset<Row> df2 = df.filter(df.col(LAT).lt(midLatitude).and(df.col(LONG).lt(midLongitude))).dropDuplicates(VEHICLE_ID, TIMEFRAME).sort(TIMEFRAME).groupBy(TIMEFRAME).count();
        Dataset<Row> df3 = df.filter(df.col(LAT).gt(midLatitude).and(df.col(LONG).lt(midLongitude))).dropDuplicates(VEHICLE_ID, TIMEFRAME).sort(TIMEFRAME).groupBy(TIMEFRAME).count();
        Dataset<Row> df4 = df.filter(df.col(LAT).lt(midLatitude).and(df.col(LONG).gt(midLongitude))).dropDuplicates(VEHICLE_ID, TIMEFRAME).sort(TIMEFRAME).groupBy(TIMEFRAME).count();
    }

    private static Map<String, Set<Integer>> map = new HashMap<>();
    void stopsPerLine(){
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
        df.filter(df.col(AT_STOP).equalTo(1)).groupBy(TIMEFRAME, "Hour", "stopID", "lineID").count().sort(TIMEFRAME,"Hour").show(50);
    }

    void busesAtStopInAreaBatch(double minLatitude, double minLongitude, double maxLatitude, double maxLongitude){
        df.filter(df.col(AT_STOP).equalTo(1)
                .and(df.col(LAT).gt(minLatitude)).and(df.col(LONG).gt(minLongitude))
                .and(df.col(LAT).lt(maxLatitude)).and(df.col(LONG).lt(maxLongitude)))
                .groupBy(TIMEFRAME, "Hour").count().sort(TIMEFRAME,"Hour").show();
    }
}
