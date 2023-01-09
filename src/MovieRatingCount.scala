import org.apache.spark.SparkContext
import org.apache.log4j.Level
import org.apache.log4j.Logger


object MovieRatingCount extends App{
  
  
  Logger.getLogger("org").setLevel(Level.ERROR)
  
  val sc = new SparkContext("local[*]","top10shopaholic")
  
  val input = sc.textFile("C:/user/abhishek/sparkinput/moviedata-201008-180523.data")
  
  val movieData = input.map(x => x.split("\t")(2)).countByValue()
  
  movieData.foreach(println)
  
  
}