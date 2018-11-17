package spark_apps;

import org.apache.commons.lang3.StringUtils;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;


public class WordcountGrepAppJava {

    private static final Logger LOGGER = Logger.getLogger(WordcountGrepAppJava.class);

    public static void main(String[] args){

        //Turn of the loggers if you what to
        Logger.getLogger("org").setLevel(Level.OFF);
        Logger.getLogger("akka").setLevel(Level.OFF);

        //Fix the file paths!
        String filename = "file:///home/stathis/IdeaProjects/untitled1/finalaa";
        String saveFile = "file:///home/stathis/results.txt";

        SparkConf conf = new SparkConf();
        conf.setAppName("spark_apps.WordcountGrepAppJava");
        conf.setMaster("local[*]");

        JavaSparkContext sc = new JavaSparkContext(conf);
        sc.setCheckpointDir("file:///home/stathis/datasets/checkpoint/");

        double split= 1d;

        if (conf.contains("spark.split")) {
            split = Double.parseDouble(conf.get("spark.split"));
        }
        JavaPairRDD<String, String> full_pair_data = sc.wholeTextFiles(filename, 2000);

        JavaRDD<Tuple2<String,String>> full_data = full_pair_data.map(line -> new Tuple2<>(line._1,line._2));
        JavaRDD<Tuple2<String, String>>[] data = full_data.randomSplit(new double[]{split, 1 - split}, 12345);


        JavaRDD<Tuple2<String, String>> lines = data[0];

        JavaRDD<Tuple2<String, String>> filteredLines = lines.filter(line ->
            Double.valueOf(line._2.split("\n")[4].split(" ")[1])>=2
        ).filter(line ->
            line._2.toLowerCase().contains("brad")
        );


        JavaRDD<Tuple2<String, Integer>> comedylinesCountMap = filteredLines.map(line -> {
            Tuple2<String, Integer> tuple2;
            if (line._2.contains("fun")) {
                tuple2 = new Tuple2<>("fun", StringUtils.countMatches(line._2.toLowerCase(), "fun"));
            } else {
                tuple2 = new Tuple2<>("fun", 0);
            }
            return tuple2;
        });


        JavaRDD<Tuple2<String, Integer>> thrillerlinesCountMap = filteredLines.map(line -> {
            if(line._2.contains("action")) {
                return new Tuple2<>("action", StringUtils.countMatches(line._2.toLowerCase(), "action"));
            } else
             return new Tuple2<>("action", 0);

        });

        JavaRDD<Tuple2<String, Integer>> totalLinesCount = comedylinesCountMap.union(thrillerlinesCountMap);


        JavaPairRDD<String, Integer> countsMap = totalLinesCount.mapToPair(line -> new Tuple2<>(line._1,line._2)).reduceByKey((x, y) -> x + y);


    countsMap.foreach(x -> System.out.println(x._1+ " " +x._2));
//        countsMap.saveAsTextFile(saveFile);
        LOGGER.info("Saved counts file file in " + saveFile);
        sc.stop();
    }


}
