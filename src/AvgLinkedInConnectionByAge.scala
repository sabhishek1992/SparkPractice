import org.apache.spark.SparkContext
import org.apache.log4j.Level
import org.apache.log4j.Logger


object AvgLinkedInConnectionByAge extends App{
  
  Logger.getLogger("org").setLevel(Level.ERROR)
  
  val sc = new SparkContext("local[*]","avgLinkedinConnByAge")
  
  val input = sc.textFile("C:/user/abhishek/sparkinput/friendsdata-201008-180523.csv")
  
  def parseLine(l: String)={
    val fields = l.split("::")
    val age = fields(2).toInt
    val numFriends = fields(3).toInt
    (age,numFriends) 
  }
  
  val mappedInput = input.map(parseLine)
  
//  mappedInput.take(10).foreach(println)
  
  val tupleInput = mappedInput.mapValues(x => (x,1))
  
  tupleInput.take(10).foreach(println)
  
  val summedUpInput = tupleInput.reduceByKey((x,y) => (x._1+y._1,x._2+y._2))
  
  val avgData = summedUpInput.map(x => (x._1,x._2._1/x._2._2))
  
//  avgData.sortBy(x=>x._2).collect.foreach(println)
  
}