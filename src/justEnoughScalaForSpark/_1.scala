package justEnoughScalaForSpark

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.log4j.Level
import org.apache.log4j.Logger
import java.io.File


//Inverted Index - Google Crawler
//For a word,file ---> count
object _1 extends App{

  Logger.getLogger("org").setLevel(Level.ERROR)
  
	val sparkConf = new SparkConf().setAppName("Scala-Spark").setMaster("local[*]")

	val sc = new SparkContext(sparkConf)

	println("Spark version: " + sc.version)
  println("Spark master: " + sc.master)
  println("Running 'locally'?: " + sc.isLocal)
	
	
  def info(message: String):String = {
		  println(message);
		  return message;
  };


	def error(message: String):String = {
	  val fullMessage = s"""
			|********************************************************************
			|
			| ERROR: $message
			|
			|********************************************************************
			|""".stripMargin;
			println(fullMessage);
			return fullMessage;
	};
	
	val infoString = info("All is well.");
  val errorString = error("Uh oh...");
  
 val shakespeare = new File("C:/Users/abc/workspace/JustEnoughScalaForSpark-master/data/shakespeare") 
  
 val success =  
   if(shakespeare.exists() == false){
     error(s"Data directory path doesn't exist! $shakespeare")
     false
   }else {
     info(s"$shakespeare exists")
     true
   }
 
 val pathSeparator = File.separator
 val targetDirName = shakespeare.toString()
 val plays = Seq(
		 "tamingoftheshrew", "comedyoferrors", "loveslabourslost", "midsummersnightsdream",
		 "merrywivesofwindsor", "muchadoaboutnothing", "asyoulikeit", "twelfthnight")

 if(success){
	 println(s"Checking that the plays are in $shakespeare:")
	 val failures = for{
		 play <- plays;
		 val playFileName = targetDirName + pathSeparator + play;
		 val playFile = new File(playFileName)
		 if(playFile.exists() == false)
	 } yield{
		 s"$playFileName:\tNOT FOUND!"
	 }

	 println("Finished!")
	 if(failures.size == 0){
		 info("All plays found!")
	 }
	 else{
		 println("Following expected plays were not found:")
		 failures.foreach(play => error(play))
	 }
 }
 
 plays.foreach(println)
 plays.foreach(println(_))
 plays.foreach(str => println) // type of str is inferred as String
 plays.foreach(str => println(str)) 
 plays.foreach((str:String) => println(str)) 
  
  }