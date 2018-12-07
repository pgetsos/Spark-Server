package spark_apps;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import static org.apache.spark.sql.functions.max;
import static org.apache.spark.sql.functions.min;
import static org.apache.spark.sql.functions.from_unixtime;

public class Queries {
    Dataset<Row> df;
    public Queries(Dataset<Row> df){
        this.df=df;
    }

    public String busesPerArea(){

        double minLongitude = (double) df.agg(min("longitude")).collectAsList().get(0).get(0);
        double maxLongitude = (double) df.agg(max("longitude")).collectAsList().get(0).get(0);
        double minLatitude = (double) df.agg(min("latitude")).collectAsList().get(0).get(0);
        double maxLatitude = (double) df.agg(max("latitude")).collectAsList().get(0).get(0);

        double midLongitude = (minLongitude + maxLongitude) / 2;
        double midLatitude = (minLatitude + maxLatitude) / 2;

        Dataset<Row> df1 = df.filter(df.col("latitude").gt(midLatitude).and(df.col("longitude").gt(midLongitude))).dropDuplicates("vehicleID", "timeFrame").sort("timeFrame").groupBy("timeFrame").count();
        Dataset<Row> df2 = df.filter(df.col("latitude").lt(midLatitude).and(df.col("longitude").lt(midLongitude))).dropDuplicates("vehicleID", "timeFrame").sort("timeFrame").groupBy("timeFrame").count();
        Dataset<Row> df3 = df.filter(df.col("latitude").gt(midLatitude).and(df.col("longitude").lt(midLongitude))).dropDuplicates("vehicleID", "timeFrame").sort("timeFrame").groupBy("timeFrame").count();
        Dataset<Row> df4 = df.filter(df.col("latitude").lt(midLatitude).and(df.col("longitude").gt(midLongitude))).dropDuplicates("vehicleID", "timeFrame").sort("timeFrame").groupBy("timeFrame").count();



        return null;
    }

    static Map<String, Set<Integer>> map = new HashMap<>();
    public String stopsPerLine(){

        df.dropDuplicates("lineID", "stopID", "atStop").filter(df.col("atStop").equalTo(1)).foreach(row -> {
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
        System.out.println(map.toString());


        return null;
    }

    public String busesAtStopBatch(){
        df.filter(df.col("atStop").equalTo(1)).groupBy("timeFrame", "Hour", "stopID", "lineID").count().sort("timeFrame","Hour").show(50);
        return null;
    }
}
