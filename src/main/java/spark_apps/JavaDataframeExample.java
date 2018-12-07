package spark_apps;

import javassist.bytecode.stackmap.TypeData;
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

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;

import static org.apache.spark.sql.functions.*;


public class JavaDataframeExample {

    private static final Logger LOGGER = Logger.getLogger(TypeData.ClassName.class.getName());

    public static void main(String[] args) {
        /*
          Level.INFO is the default. Use LOGGER.info to log things we always want to show, LOGGER.debug to show things
          while debugging e.g. results of a loop, LOGGER.warn to show ONLY things you want without all the rest
          cluttering everything (and change the below to Level.WARN). Level.OFF disables all logs (not Spark prints).
         */
        LOGGER.setLevel(Level.WARN);

        final String finishedQuery = "QUERY #%d complete in %d seconds";

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

        LOGGER.info("Loading data...");
        long start = System.currentTimeMillis();

        Dataset<Row> df = sparkSession.read().schema(schema).csv("C:\\Users\\pgetsos\\Desktop\\MSc\\sir010113-310113")
                .toDF("timestamp","lineID", "direction", "journeyID", "timeFrame", "vehicleJourneyID", "operator",
                        "congestion", "longitude", "latitude", "delay", "blockID", "vehicleID", "stopID", "atStop");

        long end = System.currentTimeMillis();
        LOGGER.info("Load complete in "+ (end - start)/1000 +" seconds");

        df = df.withColumn("DateTime", from_unixtime(df.col("timestamp").divide(lit(1000000L))));
        df = df.withColumn("Hour", hour(df.col("DateTime")));

        Queries queries = new Queries(df);

        boolean run = true;
        while(run) {
            BufferedReader br = new BufferedReader(new InputStreamReader(System.in));
            System.out.println("Choose a query:\n0) Print schema\n1) Buses per Area\n2) \n3) Stops per line\n" +
                    "4) Buses at Stop\n5) Buses at Stop in Area\n6) \n9) Exit");
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
                    break;
                case 3:
                    queries.stopsPerLine();
                    break;
                case 4:
                    queries.busesAtStopBatch();
                    break;
                case 5:
                    queries.busesAtStopInAreaBatch(53.295563, -6.323346, 53.416634, -6.297);
                    break;
                case 6:
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


    }
}
