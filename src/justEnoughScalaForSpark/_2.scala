package justEnoughScalaForSpark

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.log4j.Level
import org.apache.log4j.Logger
import java.io.File
import org.apache.spark.sql.SparkSession


//Inverted Index - Google Crawler
//For a word,file ---> count
object _2 extends App{

  Logger.getLogger("org").setLevel(Level.ERROR)
  
	val sparkConf = new SparkConf().setAppName("Scala-Spark").setMaster("local[*]")

	val sc = new SparkContext(sparkConf)

  val spark = SparkSession.builder().config(sparkConf).enableHiveSupport().getOrCreate()
  
  val shakespeare = new File("C:/Users/abc/workspace/JustEnoughScalaForSpark-master/data/shakespeare")
  
  val fileContent = sc.wholeTextFiles(shakespeare.toString())
  
  val wordFileNameOnes = fileContent.flatMap{//filename, content
    location_contents_tuple2 =>
      val words = location_contents_tuple2._2.split("""\W+""")
      .filter(word => word.size > 0)
      val fileName = location_contents_tuple2._1.split("/").last
      words.map(word => ((word.toLowerCase,fileName),1))      
  }
  
  val uniques = wordFileNameOnes.reduceByKey((count1,count2) => count1 + count2)
  
  val words = uniques.map{
    word_file_count_tup3 => 
      (word_file_count_tup3._1._1,(word_file_count_tup3._1._2,word_file_count_tup3._2))
  }
  
  val wordGroups = words.groupByKey
  
  val finalRdd = wordGroups.mapValues{
    iterable => 
    val vect = iterable.toVector.sortBy{
      //sortBy the counts descending first, then sort by the filenames.
      file_count_tup2 => (-file_count_tup2._2,file_count_tup2._1)
  }
    vect.mkString(",")
 }
  
  finalRdd.take(10).foreach(println)
 
  val df = spark.createDataFrame(finalRdd).toDF("word","locations_counts")
  df.show(50,false)
  
}