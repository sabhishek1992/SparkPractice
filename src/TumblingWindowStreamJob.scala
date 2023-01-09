import org.apache.log4j.Level
import org.apache.log4j.Logger
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.functions.from_json
import org.apache.spark.sql.functions.sum
import org.apache.spark.sql.functions.window
import org.apache.spark.sql.streaming.OutputMode
import org.apache.spark.sql.streaming.Trigger
import org.apache.spark.sql.types.IntegerType
import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.types.StructField
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.types.TimestampType


object TumblingWindowStreamJob extends App{
 
  Logger.getLogger("org").setLevel(Level.ERROR)
  
  val spark = SparkSession.builder().master("local[2]")
  .appName("My Streaming Application")
  .config("spark.sql.shuffle.partitions",3)
  .config("spark.streaming.stopGracefullyOnShutdown","true")
  .config("spark.sql.streaming.schemaInference","true")
  .getOrCreate()
  
  val ordersDF = spark.readStream.format("socket")
  .option("host", "localhost")
  .option("port","12345")
  .load()
  
//  ordersDF.printSchema() //value - string(nullable - true)

  //process
  
  val orderSchema = StructType(List(
  StructField("order_id",IntegerType),
  StructField("order_date",TimestampType),
  StructField("order_customer_id",IntegerType),
  StructField("order_status",StringType),
  StructField("amount",IntegerType)
  ))
  
  val valueDf = ordersDF.select(from_json(col("value"),
      orderSchema).alias("value"))

//  valueDf.printSchema()
  
  val refinedOrdersDf = valueDf.select("value.*")
  
//  refinedOrdersDf.printSchema()
  
  val windowAggDf = refinedOrdersDf
  .withWatermark("order_date", "30 minute")
  .groupBy(window(col("order_date"), "15 minute","5 minute"))
  .agg(sum("amount").alias("totalInvoice"))
  
  
  val outputDf = windowAggDf.select("window.start","window.end","totalInvoice")
  
  val ordersQuery = outputDf.writeStream
  .format("console")
  .outputMode(OutputMode.Update())
  .option("checkpointLocation", "checkpoint-location1")
  .trigger(Trigger.ProcessingTime("15 seconds"))
  .start()
  
  ordersQuery.awaitTermination()
}