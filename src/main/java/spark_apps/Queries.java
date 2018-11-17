package spark_apps;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

public class Queries {
    Dataset<Row> df;
    public Queries(Dataset<Row> df){
        this.df=df;
    }

    public String busesPerArea(){
        return null;
    }

    public String stopsPerLine(){
        long l = df.dropDuplicates("lineID", "stopID", "atStop").filter(df.col("lineID").equalTo("25")).filter(df.col("atStop").equalTo(1)).count();
        //df.dropDuplicates("lineID", "stopID", "atStop").filter(df.col("lineID").equalTo("25")).filter(df.col("atStop").equalTo(1)).show(30);
        df.dropDuplicates("lineID").sort("lineID").show(90);
        System.out.println(l);
        return null;
    }
}
