import org.apache.spark.SparkContext
import org.apache.log4j.Level
import org.apache.log4j.Logger
import org.apache.spark.sql.SparkSession
import org.apache.spark.SparkConf


object StudentsPreProcessing extends App{
  
  Logger.getLogger("org").setLevel(Level.ERROR);
  
  val sparkConf = new SparkConf
	sparkConf.set("spark.app.name", "df")
	sparkConf.set("spark.master", "local[*]")
  
  val spark = SparkSession.builder().config(sparkConf)
  .getOrCreate()

  import spark.implicits._
  
  val studentsDf = spark.read.format("csv").option("inferSchema", true)
  .option("header", true)
  .option("path", "C://user//abhishek//sparkinput//students_with_header-201214-111236.csv")
  .load()
  
  studentsDf.write.partitionBy("subject").parquet("C://user//abhishek//sparkoutput//students_data//");
  
  spark.stop()
}