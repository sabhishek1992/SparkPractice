import org.apache.log4j.Level
import org.apache.log4j.Logger
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.streaming.Seconds
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.streaming.OutputMode
import org.apache.spark.sql.streaming.Trigger


object StruturedStreamingWordCount extends App{

  Logger.getLogger("org").setLevel(Level.ERROR)
  
  val spark = SparkSession.builder().master("local[2]")
  .appName("My Streaming Application")
  .config("spark.sql.shuffle.partitions",3)
  .config("spark.streaming.stopGracefullyOnShutdown","true")
  .getOrCreate()
  
  //1. read from the stream
  val linesDF = spark.readStream.format("json")
  .option("host", "localhost")
  .option("port","9999")
  .load()
  
  //check schema
  //linesDF.printSchema() ------ value - String
  
  //2. process
  val wordsDF = linesDF.selectExpr("explode(split(value,' ')) as word")
  val wordCountDF = wordsDF.groupBy("word").count()
  
  //3. write to the sink
  val wordCountQuery = wordCountDF.writeStream.format("console")
  .outputMode(OutputMode.Complete())
  .option("checkpointLocation","checkpoint-location1")
  .trigger(Trigger.ProcessingTime("30 seconds"))
  .start()
  
  wordCountQuery.awaitTermination()
}