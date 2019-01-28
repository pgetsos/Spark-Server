package spark_apps;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.sql.*;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.Arrays;

import static org.apache.spark.sql.functions.*;



public class MainClass {
    private static final Logger LOGGER = Logger.getLogger("MainClass");

    public static void main(String[] args) {
        /*
          Level.INFO is the default. Use LOGGER.info to log things we always want to show, LOGGER.debug to show things
          while debugging e.g. results of a loop, LOGGER.warn to show ONLY things you want without all the rest
          cluttering everything (and change the below to Level.WARN). Level.OFF disables all logs (not Spark prints).
         */
        LOGGER.setLevel(Level.INFO);

        final String finishedQuery = "QUERY #%d complete in %d seconds";

        Logger.getLogger("org").setLevel(Level.OFF);
        Logger.getLogger("akka").setLevel(Level.OFF);
        SparkConf conf = new SparkConf();
        conf.setAppName("spark_apps.MainClass");
        conf.set("spark.driver.allowMultipleContexts", "true");

        conf.setMaster("local[*]");
        JavaSparkContext sc = new JavaSparkContext(conf);


//        JavaStreamingContext jssc = new JavaStreamingContext(conf, Durations.seconds(1));


        SparkSession sparkSession = SparkSession.builder().sparkContext(sc.sc()).getOrCreate();
//        SparkSession sparkSessionStreaming = SparkSession.builder().sparkContext(jssc.ssc().sparkContext()).getOrCreate();

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

        LOGGER.info("Loading data...");
        long start = System.currentTimeMillis();

        String path = "C:\\Users\\pgetsos\\Desktop\\MSc\\sir010113-310113"; // Petros
        String path2 = "/media/spiros/Data/SparkDataset/"; // Spiros
        String path3 = "/Users/jason/Desktop/dataset/"; // Iasonas

        Dataset<Row> df = sparkSession.read().schema(schema).csv(path2)
                .toDF("timestamp","lineID", "direction", "journeyID", "timeFrame", "vehicleJourneyID", "operator",
                        "congestion", "longitude", "latitude", "delay", "blockID", "vehicleID", "stopID", "atStop");


//        Dataset<Row> dfStream = sparkSession.readStream().schema(schema).csv(path2)
//                .toDF("timestamp","lineID", "direction", "journeyID", "timeFrame", "vehicleJourneyID", "operator",
//                        "congestion", "longitude", "latitude", "delay", "blockID", "vehicleID", "stopID", "atStop");


//        JavaReceiverInputDStream<String> lines = jssc.socketTextStream("localhost", 4321);

//        JavaDStream<String> columns = lines.flatMap(x -> Arrays.asList(x.split(",")).iterator());

//        lines.foreachRDD((VoidFunction<JavaRDD<String>>) rdd -> {
//                    JavaRDD<Row> rowRDD = rdd.map((Function<String, Row>) msg -> {
//                        Row row = RowFactory.create(msg);
//                        return row;
//                    });
//                    //Create Schema
//                    //StructType schema1 = DataTypes.createStructType(new StructField[] {DataTypes.createStructField("Message", DataTypes.StringType, true)});
//                    //Get Spark 2.0 session
//
//                    Dataset<Row> msgDataFrame = sparkSession.createDataFrame(rowRDD, schema);
//                    msgDataFrame.show();
//                });



    Dataset<Row> dfStream = sparkSession
            .readStream()
//            .schema(schema)
            .format("socket")
            .option("host", "localhost")
            .option("port", 4321)
            .load();

        dfStream = dfStream
                .withColumn("timestamp", split(col("value"), ",").getItem(0))
                .withColumn("lineID", split(col("value"), ",").getItem(1))
                .withColumn("direction", split(col("value"), ",").getItem(2))
                .withColumn("journeyID", split(col("value"), ",").getItem(3))
                .withColumn("timeFrame", split(col("value"), ",").getItem(4))
                .withColumn("vehicleJourneyID", split(col("value"), ",").getItem(5))
                .withColumn("operator", split(col("value"), ",").getItem(6))
                .withColumn("congestion", split(col("value"), ",").getItem(7))
                .withColumn("longitude", split(col("value"), ",").getItem(8))
                .withColumn("latitude", split(col("value"), ",").getItem(9))
                .withColumn("delay", split(col("value"), ",").getItem(10))
                .withColumn("blockID", split(col("value"), ",").getItem(11))
                .withColumn("vehicleID", split(col("value"), ",").getItem(12))
                .withColumn("stopID", split(col("value"), ",").getItem(13))
                .withColumn("atStop", split(col("value"), ",").getItem(14));


        // Split the lines into words
        //Dataset<String> columns = dfStream
//                .as(Encoders.STRING())
//
//                .flatMap((FlatMapFunction<String, String>) x -> Arrays.asList(x.split(",")).iterator(), Encoders.STRING());

    long end = System.currentTimeMillis();
    LOGGER.info("Load complete in "+ (end - start)/1000 +" seconds");

    df = df.withColumn("DateTime", from_utc_timestamp(to_utc_timestamp(from_unixtime(df.col("timestamp").divide(lit(1000000L))), "Europe/Athens"), "Europe/Dublin"));
    df = df.withColumn("Date", date_format(df.col("DateTime"), "yyyy-MM-dd"));
    df = df.withColumn("Hour", hour(df.col("DateTime")));

    dfStream = dfStream.withColumn("DateTime", from_utc_timestamp(to_utc_timestamp(from_unixtime(col("timestamp").divide(lit(1000000L))), "Europe/Athens"), "Europe/Dublin"));
    dfStream = dfStream.withColumn("Date", date_format(col("DateTime"), "yyyy-MM-dd"));
    dfStream= dfStream.withColumn("Hour", hour(col("DateTime")));


    Queries queries = new Queries(df);
    Queries streaming_queries = new Queries(dfStream);




    boolean run = true;
    while(run) {
        BufferedReader br = new BufferedReader(new InputStreamReader(System.in));
        System.out.println("Choose a query:\n0) Print schema\n1) Buses per Area\n2) Congested Buses per Day per Hour per Area\n3) Stops per line\n" +
                "4) Buses at Stop\n5) Buses at Stop in Area\n6) Time to Stop\n9) Exit");
        int a;
        try {
            a = Integer.parseInt(br.readLine());
        } catch (IOException e) {
            System.out.println("Wrong input, please try again");
            continue;
        }
        start = System.currentTimeMillis();
        switch (a){
            case 0:
                df.printSchema();
                continue;
            case 1:
                queries.busesPerArea();
                break;
            case 2:
                queries.congestedBuses();
                break;
            case 3:
                queries.stopsPerLine();
                break;
            case 4:
                try {
                    System.out.println("Choose a date (YYYY-MM-DD format)");
                    String date = br.readLine();
                    System.out.println("Choose Hour");
                    int hour = Integer.parseInt(br.readLine());
                    System.out.println("Choose a stopID");
                    int stopID = Integer.parseInt(br.readLine());
                    streaming_queries.busesAtStopStreaming(date, hour, stopID);
//                    jssc.start();
//                    try{
//                        jssc.awaitTermination();
//                    } catch (Exception e){
//                        e.printStackTrace();
//                    }


                } catch (IOException e) {
                    System.out.println("Wrong input, please try again");
                    continue;
                }
                break;
            case 5:
                try {
                    System.out.println("Choose minimum latitude");
                    double minlat = Double.parseDouble(br.readLine());
                    System.out.println("Choose maximum latitude");
                    double maxlat = Double.parseDouble(br.readLine());

                    System.out.println("Choose minimum longitude");
                    double minlon = Double.parseDouble(br.readLine());
                    System.out.println("Choose maximum longitude");
                    double maxlon = Double.parseDouble(br.readLine());

                    queries.busesAtStopInAreaBatch(minlat, minlon, maxlat, maxlon);
                } catch (IOException e) {
                    System.out.println("Wrong input, please try again");
                    continue;
                }
                break;
            case 6:
                try {
                    System.out.println("Choose lineID");
                    String lineID = br.readLine();
                    System.out.println("Choose a date (YYYY-MM-DD format)");
                    String date = br.readLine();
                    System.out.println("Choose a stopID");
                    int stopID = Integer.parseInt(br.readLine());
                    queries.timeToStop(lineID, date,stopID);
                } catch (IOException e) {
                    System.out.println("Wrong input, please try again");
                    continue;
                }
                break;
            case 7:
                break;
            case 9:
                run = false;
                continue;
            default:
                System.out.println("Wrong input, please try again");
                break;
        }
        end = System.currentTimeMillis();
        LOGGER.info(String.format(finishedQuery, a, (end - start) / 1000));
    }
}}
