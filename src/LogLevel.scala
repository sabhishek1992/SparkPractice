import org.apache.log4j.Level
import org.apache.log4j.Logger
import org.apache.spark.SparkContext


object LogLevel extends App{
  
  Logger.getLogger("org").setLevel(Level.ERROR)
  
  val sc = new SparkContext("local[*]","loglevel")
  
  println(sc.defaultParallelism)
  
  /*val baseRdd = sc.textFile("C:/user/abhishek/sparkinput/bigLog.txt")
  
  val mappedRdd = baseRdd.map(x => {
    val fields = x.split(":")
    (fields(0),1)
  })
  
  mappedRdd.reduceByKey((x,y) => x+y).collect
  .foreach(println)*/
  
  val myList = List("WARN: Tuesday 4 September 0405",
      "ERROR: Tuesday 4 September 0408",
      "ERROR: Tuesday 4 September 0408",
      "ERROR: Tuesday 4 September 0408",
      "ERROR: Tuesday 4 September 0408")
      
  val myRdd = sc.parallelize(myList)
  
  println("no of partitions : "+myRdd.getNumPartitions)
  
  val mappedRdd = myRdd.map(x => {
    val a= x.split(":")
    (a(0),1)
  })
  
  val logLevelRdd = mappedRdd.reduceByKey((x,y) => x+y)
  
  logLevelRdd.collect.foreach(println)
  
  scala.io.StdIn.readLine()
  
 /* val warnAcc = sc.longAccumulator("warn log level accumulator")
  val errorAcc = sc.longAccumulator("error log level accumulator")
  
  myRdd.foreach(x => if(x.contains("WARN")) warnAcc.add(1))
  myRdd.foreach(x => if(x.contains("ERROR")) errorAcc.add(1))
  
  println("warnAcc: "+warnAcc.value + ", errorAcc: "+errorAcc.value)*/
}