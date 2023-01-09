import org.apache.log4j.Level
import org.apache.log4j.Logger
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.IntegerType
import org.apache.spark.sql.types.LongType
import org.apache.spark.sql.types.StructField
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.types.TimestampType
import org.apache.spark.sql.functions._
import org.apache.spark.sql.streaming.OutputMode
import org.apache.spark.sql.streaming.Trigger

object StreamJoinsJob extends App{
 
  Logger.getLogger("org").setLevel(Level.ERROR)
  
  val spark = SparkSession.builder().master("local[2]")
  .appName("My Streaming Application")
  .config("spark.sql.shuffle.partitions",3)
  .config("spark.streaming.stopGracefullyOnShutdown","true")
  .config("spark.sql.streaming.schemaInference","true")
  .getOrCreate()
  
  val transactionSchema = StructType(List(
    StructField("card_id",LongType),
    StructField("amount",IntegerType),
    StructField("postcode",IntegerType),
    StructField("pos_id",LongType),
    StructField("transaction_dt",TimestampType)
  ))
  
  //load stream DF
  val transactionDf = spark.readStream.format("socket")
  .option("host","localhost")
  .option("port","12345")
  .load() // value
  
  val valueDf = transactionDf
  .select(from_json(col("value"), transactionSchema).alias("value"))
  
  val refinedTransactionDf = valueDf.select("value.*")
  
//  refinedTransactionDf.printSchema()
  
  //load static DF
  
  val memberDf = spark.read.format("csv")
  .option("header",true)
  .option("inferSchema",true)
  .option("path","C:/user/abhishek/sparkinput/member_details.csv")
  .load()
  
  val enrichedDf = refinedTransactionDf.join(memberDf,
   refinedTransactionDf.col("card_id") === memberDf.col("card_id") ,
   "inner").drop(memberDf.col("card_id"))
   
   //write to the sink
  val transactionQuery = enrichedDf.writeStream
  .format("console")
  .outputMode(OutputMode.Update())
  .option("checkpointLocation", "checkpoint-location1")
  .trigger(Trigger.ProcessingTime("15 second"))
  .start()

  transactionQuery.awaitTermination()
}