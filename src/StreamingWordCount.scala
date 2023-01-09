import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.Seconds
import org.apache.log4j.Logger
import org.apache.log4j.Level


object StreamingWordCount extends App{
  
  Logger.getLogger("org").setLevel(Level.ERROR)
  
  /*def updateFunc(newValues:Seq[Int],previousState: Option[Int]): Option[Int] = {
    val newCount =  previousState.getOrElse(0) + newValues.sum
    Some(newCount)
  }*/
  
  //creating spark streaming context
  val conf = new SparkConf().setAppName("streamingwordcount").setMaster("local[*]")
  val sc = new SparkContext(conf)
  val ssc = new StreamingContext(sc,Seconds(2))
  
  def summaryFunc(x:String,y:String) = {
    (x.toInt + y.toInt).toString()
  }
  
  def inverseFunc(x:String,y:String) = {
    (x.toInt - y.toInt).toString()
  }
  
  ssc.checkpoint(".")
  //lines is a dstream
  val lines = ssc.socketTextStream("127.0.0.1", 9995)
  
  //words is a transformed dstream
//  val words = lines.flatMap(x => x.split(" "))
  
//  val pairs = words.map(x => (x,1))
  
//  val wordCounts = pairs.updateStateByKey(updateFunc)
  
  val wordCounts = lines.countByWindow(Seconds(10), Seconds(2))
  
  wordCounts.print()
  
  ssc.start()
  
  ssc.awaitTermination()
}