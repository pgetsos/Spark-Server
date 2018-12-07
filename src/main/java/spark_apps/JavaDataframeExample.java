package spark_apps;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import java.util.List;

import static org.apache.spark.sql.functions.*;
import static org.apache.spark.sql.types.DataTypes.DateType;
import static org.apache.spark.sql.types.DataTypes.FloatType;


public class JavaDataframeExample {


    public static void main(String[] args) {


        Logger.getLogger("org").setLevel(Level.OFF);
        Logger.getLogger("akka").setLevel(Level.OFF);
        SparkConf conf = new SparkConf();
        conf.setAppName("spark_apps.JavaDataframeExample");
        conf.setMaster("local[*]");
        JavaSparkContext sc = new JavaSparkContext(conf);
        SparkSession sparkSession = SparkSession.builder().sparkContext(sc.sc()).getOrCreate();
        //sc.setCheckpointDir("file:///home/stathis/datasets/checkpoint/");

        StructType schema = new StructType(new StructField[]{
                new StructField("timestamp", DataTypes.LongType, false, Metadata.empty()),
                new StructField("lineID", DataTypes.StringType, true, Metadata.empty()),
                new StructField("direction", DataTypes.IntegerType, true, Metadata.empty()),
                new StructField("journeyID", DataTypes.StringType, true, Metadata.empty()),
                new StructField("timeFrame", DataTypes.DateType, true, Metadata.empty()),
                new StructField("vehicleJourneyID", DataTypes.FloatType, true, Metadata.empty()),
                new StructField("operator", DataTypes.StringType, true, Metadata.empty()),
                new StructField("congestion", DataTypes.IntegerType, true, Metadata.empty()),
                new StructField("longitude", DataTypes.DoubleType, true, Metadata.empty()),
                new StructField("latitude", DataTypes.DoubleType, true, Metadata.empty()),
                new StructField("delay", DataTypes.IntegerType, true, Metadata.empty()),
                new StructField("blockID", DataTypes.IntegerType, true, Metadata.empty()),
                new StructField("vehicleID", DataTypes.IntegerType, true, Metadata.empty()),
                new StructField("stopID", DataTypes.IntegerType, true, Metadata.empty()),
                new StructField("atStop", DataTypes.IntegerType, true, Metadata.empty())
        });

        System.out.println("Loading data...");
        long start = System.currentTimeMillis();
        //Dataset<Row> df = sparkSession.read().json("people.json");
        Dataset<Row> df = sparkSession.read().schema(schema).csv("C:\\Users\\pgetsos\\Desktop\\MSc\\sir010113-310113")
                .toDF("timestamp","lineID", "direction", "journeyID", "timeFrame", "vehicleJourneyID", "operator",
                        "congestion", "longitude", "latitude", "delay", "blockID", "vehicleID", "stopID", "atStop");
        long end = System.currentTimeMillis();
        System.out.println("Load complete in "+ (end - start)/1000 +" seconds");


        df.printSchema();

//        df = df.withColumn( "delay", df.col("delay").cast(FloatType));
//        df = df.withColumn( "delay", df.col("delay").cast(FloatType));
        //df.show(5);
//        df.filter(df.col("lineID").equalTo("40")).filter(df.col("vehicleID").equalTo(33142)).sort("timestamp").show(30);//show(30);
        //df.agg(min("delay")).show();

// Print the schema in a tree format
        //df.printSchema();

//// Select only the "name" column
//        df.select("name").show();
//
//
//// Select everybody, but increment the age by 1
//        df.select(df.col("name"), df.col("age").plus(1)).show();
//
//
//// Select people older than 21
//        df.filter(df.col("age").gt(21)).show();
//
//        df.groupBy("age").count().show();
//        df.createOrReplaceTempView("people");
//
//        Dataset<Row> sqlDF = sparkSession.sql("SELECT * FROM people");
//        sqlDF.show();
        Queries queries = new Queries(df);
        System.out.println("QUERY #1");
        start = System.currentTimeMillis();
        queries.busesPerArea();
        end = System.currentTimeMillis();
        System.out.println("QUERY #1 complete in "+ (end - start)/1000 +" seconds");
        System.out.println("QUERY #2");

        System.out.println("QUERY #3");
        start = System.currentTimeMillis();
        queries.stopsPerLine();
        end = System.currentTimeMillis();
        System.out.println("QUERY #3 complete in "+ (end - start)/1000 +" seconds");

    }
    }
