package justEnoughScalaForSpark

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.log4j.Level
import org.apache.log4j.Logger
import java.io.File
import org.apache.spark.sql.SparkSession


//Inverted Index - Google Crawler
//For a word,file ---> count
object _3 extends App{

  Logger.getLogger("org").setLevel(Level.ERROR)
  
	val sparkConf = new SparkConf().setAppName("Scala-Spark").setMaster("local[*]")

	val sc = new SparkContext(sparkConf)

  val spark = SparkSession.builder().config(sparkConf).enableHiveSupport().getOrCreate()
  
  val shakespeare = new File("C:/Users/abc/workspace/JustEnoughScalaForSpark-master/data/shakespeare")
  
  val fileContent = sc.wholeTextFiles(shakespeare.toString())
  
  
  //The case keyword says I want to pattern match on
  //the object passed to the function.
  //if it's 2-element tuple, then extract elements and assign it to variables
  
  val wordFileNameOnes = fileContent.flatMap{//location, content
    
    case(location,"") =>
      Array.empty[((String,String),Int)]
    
    case(location,content) =>
      val words = content.split("""\W+""")
      .filter(word => word.size > 0)
      val fileName = location.split("/").last
      words.map(word => ((word.toLowerCase,fileName),1))      
  }
  
  val uniques = wordFileNameOnes.reduceByKey(_+_)
  
  val words = uniques.map{
    case((word, fileName),count) => (word,(fileName,count))
  }
  
  val wordGroups = words.groupByKey.sortByKey(true)
  
  val finalRdd = wordGroups.map{
    
    case (word,iterable) => 
    
    val vect = iterable.toVector.sortBy{
      //sortBy the counts descending first, then sort by the filenames.
      case(fileName, count) => (-count,fileName)
    }
    
    val (locations,counts) = vect.unzip
    
    val totalCount = counts.reduceLeft(_+_)
    
    (word,totalCount,locations,counts)
 }
  
  val df = spark.createDataFrame(finalRdd).toDF("word","totalCount","locations","counts")
  
//  df.show(numRows=50, truncate=false)
  
  df.createOrReplaceTempView("inverted_index")
  
  df.printSchema()
  
  val topLocation = spark.sql("""select word,totalCount,locations[0] as top_location,counts[0] as top_count from inverted_index """)
  
  topLocation.show(50,false)
  
  
  
  
  
  
  
  
  
  
  
  
  
  
  
  
  
  
  
}