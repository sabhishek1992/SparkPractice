import org.apache.spark.SparkContext
import org.apache.log4j.Level
import org.apache.log4j.Logger
import org.apache.spark.storage.StorageLevel


object TotalSpent extends App{
  
  Logger.getLogger("org").setLevel(Level.ERROR)
  
  val sc = new SparkContext("local[*]","totalspent")
  
  val input = sc.textFile("C:/user/abhishek/sparkinput/customerorders-201008-180523.csv")
   
  val mappedInput = input.map(x => {
    val a = x.split(","); (a(0),a(2).toDouble)
  })   
  
  println(mappedInput.toDebugString)
  
  val totalByCustomer = mappedInput.reduceByKey((x,y) => x+y)
  
  val premiumCustomers = totalByCustomer.filter(x=> x._2 > 5000)
  
  val doubleAmount = premiumCustomers
  .map(x => (x._1,x._2*2)).persist(StorageLevel.MEMORY_AND_DISK_SER)
  
//  doubleAmount.collect.foreach(println)
  
//  println(doubleAmount.count)
  
//  println(doubleAmount.toDebugString)
  
  scala.io.StdIn.readLine()
}