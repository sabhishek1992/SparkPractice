

import org.apache.log4j.Level
import org.apache.log4j.Logger
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.DateType
import org.apache.spark.sql.types.LongType
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.SaveMode


object DataFramesExample extends App{
 
  case class Logging(level:String, datetime:String)
  
  def mapper(line: String):Logging = {
    val fields = line.split(",")
    
    val logs:Logging = Logging(fields(0),fields(1))
    
    return logs
  }
  
	Logger.getLogger("org").setLevel(Level.ERROR)
	
	val sparkConf = new SparkConf
	sparkConf.set("spark.app.name", "df")
	sparkConf.set("spark.master", "local[*]")
	
	val spark = SparkSession.builder().config(sparkConf)
//	.enableHiveSupport()
	.getOrCreate()

	import spark.implicits._
	

val ds1 = spark.read
.option("header",true)
.csv("C:/user/abhishek/sparkinput/biglog-201105-152517.txt")
  

ds1.write.format("csv").mode(SaveMode.Overwrite).saveAsTable("biglog");

/*ds1.createOrReplaceTempView("logging_table")

val months = List("January","February","March","April","May","June","July","August",
     "September","October","November","December")

val df2 = spark.sql("""select level, 
  date_format(datetime,'MMMM') as month,cast(date_format(datetime,'M')
   as int) as monthNum from logging_table 
   """).groupBy("level").pivot("month",months).count*/
   
   
   
 /*df2.coalesce(1).write.format("csv")
 .mode(SaveMode.Overwrite).option("header",true)
 .option("path", "C:/user/abhishek/sparkinput/logging.csv")
 .save()*/
   
//group by level,month order by monthNum,level   
//df2.createOrReplaceTempView("new_log_table")
  
 
//df2.show(100)
//df2.printSchema()
 
//	scala.io.StdIn.readLine()
	
	spark.stop()
}