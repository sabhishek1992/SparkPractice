import org.apache.spark.SparkContext


import org.apache.log4j.Level
import org.apache.log4j.Logger



object WordCount extends App{
  
  Logger.getLogger("org").setLevel(Level.ERROR);
  
  val sc = new SparkContext() //"local[*]","wordcount"
  
  val input = sc.textFile("s3n://abhishek-sharma-1992/search.txt")
  
  val words = input.flatMap(_.split(" "))
  
  val wordsInUpper = words.map(_.toUpperCase())
  
  val wordMap = wordsInUpper.map((_,1))
  
  val wordWithCount = wordMap.reduceByKey(_+_)

  val output = wordWithCount.map(x => (x._2,x._1)).sortByKey(false)
  
  output.map(x => (x._1,x._2)).take(10).foreach(println)
  
  scala.io.StdIn.readLine()
  
  
}