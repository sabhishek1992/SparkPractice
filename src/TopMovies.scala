import org.apache.spark.SparkContext
import org.apache.log4j.Level
import org.apache.log4j.Logger


object TopMovies extends App{
  
  Logger.getLogger("org").setLevel(Level.ERROR)
  
  val sc = new SparkContext("local[*]","top10shopaholic")
  
  
  /*  var movieNames = sc.broadcast(sc.textFile("C:/user/abhishek/sparkinput/movies-201019-002101.dat").map(x => {
      val movieDetails = x.split("::")
      (movieDetails(0),movieDetails(1))
    }).collectAsMap())
*/
  
 
  
  //1::1193::5::978300760
  val ratingsRDD = sc.textFile("C:/user/abhishek/sparkinput/ratings-201019-002101.dat")
  val ratingsRepRDD = ratingsRDD.repartition(1)
  
  ratingsRepRDD.map(x => {
    val fields = x.split("::")
    (fields(1),(fields(2).toDouble,1)) //(1193,(5,1))
  })
  .reduceByKey((x,y) => (x._1+y._1,x._2+y._2))
  .filter(x => x._2._2 >= 1000)
  .map(x => (x._1,(x._2._1/x._2._2)))
  .filter(x => x._2 > 4.5)
  .take(20).foreach(println)
//  
  
  /* val movieRdd = sc.textFile("C:/user/abhishek/sparkinput/movies-201019-002101.dat").map(x => {
      val movieDetails = x.split("::")
      (movieDetails(0),movieDetails(1))
    }).join(ratingsRDD)
     .map(x => {
   (x._2._2,x._2._1)
 })*/
// .collect.foreach(println) 
 
 
 
 scala.io.StdIn.readLine()
 
}