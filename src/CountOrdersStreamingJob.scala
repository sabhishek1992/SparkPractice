import org.apache.spark.sql.SparkSession
import org.apache.log4j.Level
import org.apache.log4j.Logger
import org.apache.spark.sql.streaming.OutputMode
import org.apache.spark.sql.streaming.Trigger


object CountOrdersStreamingJob extends App{
 
  Logger.getLogger("org").setLevel(Level.ERROR)
  
  val spark = SparkSession.builder().master("local[2]")
  .appName("My Streaming Application")
  .config("spark.sql.shuffle.partitions",3)
  .config("spark.streaming.stopGracefullyOnShutdown","true")
  .config("spark.sql.streaming.schemaInference","true")
  .getOrCreate()
  
  //1. read from the stream
  val ordersDF = spark.readStream.format("json")
  .option("path","myinputfolder")
  .option("maxFilesPerTrigger",1)
  .load()
  
  //2. process
  ordersDF.createOrReplaceTempView("orders")
  
  val completedOrdersDF = spark.sql("select count(*) from orders where order_status='COMPLETE'")
  
  //3. write to a file
  val ordersQuery = completedOrdersDF.writeStream
  .format("console")
  .outputMode(OutputMode.Complete())
//  .option("path","myoutputfolder")
  .option("checkpointLocation","checkpoint-location2")
  .trigger(Trigger.ProcessingTime("15 seconds"))
  .start()
  
  ordersQuery.awaitTermination()  
  
}