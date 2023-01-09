import org.apache.spark.sql.types.StringType
import org.apache.log4j.Level
import org.apache.log4j.Logger
import org.apache.spark.sql.types.TimestampType
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.types.StructField
import org.apache.spark.sql.functions._
import org.apache.spark.sql.streaming.OutputMode
import org.apache.spark.sql.streaming.Trigger


object StreamToStreamJoinJob extends App{
 
  Logger.getLogger("org").setLevel(Level.ERROR)
  
  val spark = SparkSession.builder().master("local[2]")
  .appName("My Streaming Application")
  .config("spark.sql.shuffle.partitions",3)
  .config("spark.streaming.stopGracefullyOnShutdown","true")
  .config("spark.sql.streaming.schemaInference","true")
  .getOrCreate()
  
  val impressionSchema = StructType(List(
    StructField("impressionID",StringType),
    StructField("ImpressionTime",TimestampType),
    StructField("CampaignName",StringType)
  ))
  
  val clickSchema = StructType(List(
    StructField("clickID",StringType),
    StructField("ClickTime",TimestampType)
  ))
  
  val impressionDf = spark.readStream.format("socket")
  .option("host","localhost")
  .option("port","12345")
  .load()
  
  val clickDf = spark.readStream.format("socket")
  .option("host","localhost")
  .option("port","12346")
  .load()
  
  val valueDf1 = impressionDf.select(from_json(col("value"),
      impressionSchema).alias("value"))
  val impressionsDfNew = valueDf1.select("value.*")
  .withWatermark("ImpressionTime", "30 minute")
      
//  impressionsDfNew.printSchema()
  
  val valueDf2 = clickDf.select(from_json(col("value"),
      clickSchema).alias("value"))
  val clicksDfNew = valueDf2.select("value.*")    
  .withWatermark("ClickTime", "30 minute")
  
//  clicksDfNew.printSchema()
  
  //join condition
  val joinExpr = expr("""impressionID === clickId AND 
    ClickTime BETWEEN ImpressionTime AND 
    ImpressionTime + interval 15 minute
    """)
  val joinType = "inner"    
      
  val joinDf = impressionsDfNew.join(clicksDfNew,joinExpr,joinType)
  .drop(clicksDfNew.col("clickID"))
      
 val query = joinDf.writeStream
  .format("console")
  .outputMode(OutputMode.Append())
  .option("checkpointLocation", "checkpoint-location1")
  .trigger(Trigger.ProcessingTime("15 second"))
  .start()

  query.awaitTermination()      
      
      
}