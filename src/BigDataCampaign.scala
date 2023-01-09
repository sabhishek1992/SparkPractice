import org.apache.spark.SparkContext
import org.apache.log4j.Level
import org.apache.log4j.Logger
import scala.io.Source


object BigDataCampaign extends App{
  
  Logger.getLogger("org").setLevel(Level.ERROR)
  
  def loadBoringWords(): Set[String] = {
    var boringWords:Set[String] = Set()
        
    Source.fromFile("/user/abhishek/sparkinput/boringwords-201014-183159.txt")
    .getLines().toSet
  }

  val sc = new SparkContext("local[*]","bigdatacampaign")
  
  var nameSet =  sc.broadcast(loadBoringWords())
  
  val input = sc.textFile("C:/user/abhishek/sparkinput/bigdatacampaigndata-201014-183159.csv")
  
  val mappedInput = input.map(x => {
    val a = x.split(",")
    (a(10).toFloat,a(0).toLowerCase())
  })
  
  val flattedMap = mappedInput.flatMapValues(x => x.split(" "))
  
  val tupleData = flattedMap.map(x => (x._2,x._1))
  
  val filteredData = tupleData.filter(x => {
    !nameSet.value(x._1)  
  })
  
  val finalResult = filteredData.reduceByKey((x,y) => x+y)
  .sortBy(x=>x._2, false)
  
  finalResult.take(20).foreach(println)
}