import org.apache.spark.SparkContext
import org.apache.log4j.Level
import org.apache.log4j.Logger


object FindNumberOfBlankLines extends App{
  
  Logger.getLogger("org").setLevel(Level.ERROR)
  
  val sc = new SparkContext("local[*]","findNumberOfBlankLines")
  
  val myAccumulator = sc.longAccumulator("blank lines accumulator")
  
  val input = sc.textFile("C:/user/abhishek/sparkinput/samplefile-201014-183159.txt")
  
  input.foreach(x => if(x=="") myAccumulator.add(1))  
  
  println("myAccumulator: "+myAccumulator.value)
}