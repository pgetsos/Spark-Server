package spark_apps

import org.apache.commons.lang3.StringUtils
import org.apache.spark.{SparkConf, SparkContext}


object WordcountGrepApp {

  /**
    * The main method of the program will receive as input
    * the file from where data will be read and the path to
    * the results file.
    *
    * @param args the two parameter (input/output) of the
    *             application,
    */
  def main(args: Array[String]): Unit = {

    //Fix the file paths!
    val filename = "file:///home/stathis/finalaa"
    val saveFile = ""

    val conf = new SparkConf()
    conf.setAppName("spark_apps.WordcountGrepApp")
    conf.setMaster("local[*]")
    val sc = new SparkContext(conf)
    sc.setCheckpointDir("file:///home/stathis/datasets/checkpoint/")

    var split: Double = 1

    if (conf.contains("spark.split")) {
      split = conf.get("spark.split").toDouble
    }
    val full_data = sc.wholeTextFiles(filename,2000)


    val Array(lines, test) = full_data.randomSplit(Array(split, 1 - split), seed = 12345)


//    val lines = train.flatMap( f => {  f._2.split("\n\n")})
    //    val Array(lines, test) = full_data.randomSplit(Array(split, 1 - split), seed = 12345)

    //    val words = lines.flatMap(line => line.split(" "))

    //    val wordsCountMap = words.map { word => (word, 1) }
    //    val countsMap = wordsCountMap.reduceByKey((x, y) => x + y)
    //    countsMap.saveAsTextFile(saveFile)
    val filteredLines = lines.filter(line=>{ line._2.split("\n")(4).split(" ")(1).toDouble>=2}).filter(line => {
      line._2.toLowerCase.contains("brad")
    })


    val comedylinesCountMap = filteredLines.map(line => {
//      val comedyCount =  StringUtils.countMatches(line.toLowerCase, "fun")
      if(line._2.contains("fun"))
        ( "fun" , StringUtils.countMatches(line._2.toLowerCase, "fun"))
      else
        ("fun",0)
    })
    val thrillerlinesCountMap = filteredLines.map{line => {
//      val comedyCount =  StringUtils.countMatches(line.toLowerCase, "brad")
      if(line._2.contains("action"))
        ( "action" , StringUtils.countMatches(line._2.toLowerCase, "action"))
      else
        ("action",0)

    }}

    val totalLinesCount = comedylinesCountMap.union(thrillerlinesCountMap)


    val countsMap  =totalLinesCount.asInstanceOf[org.apache.spark.rdd.RDD[(String,Int)]].reduceByKey((x,y)=> x+y)






    //    val words = lines.flatMap(line => line.split(" "))
    //
    //        val wordsCountMap = words.map { word => (word, 1) }
    //        val countsMap = wordsCountMap.reduceByKey((x, y) => x + y)


    countsMap.foreach(x => print(x._1+ " " +x._2+" "))
//    countsMap.saveAsTextFile(saveFile)
//    countsMap.saveAsHadoopFile(saveFile)
    sc.stop()
  }
}
