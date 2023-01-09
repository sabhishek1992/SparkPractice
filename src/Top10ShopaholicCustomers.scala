import org.apache.spark.SparkContext
import org.apache.log4j.Logger
import org.apache.log4j.Level


object Top10ShopaholicCustomers extends App{
  
  Logger.getLogger("org").setLevel(Level.ERROR)
  
  val sc = new SparkContext("local[*]","top10shopaholic")
  
  val input = sc.textFile("C:/user/abhishek/sparkinput/customerorders-201008-180523.csv")
  
  println("no of partition : "+input.getNumPartitions)
  
  println("increased partitions : "+input.coalesce(1).getNumPartitions)
  
  val purchase = input.map(x => {
    val a = x.split(","); (a(0),a(2).toDouble)
    })
  
  val customersAmtSpent = purchase.reduceByKey((x,y) => (x+y))
  
  val spentGrt5000 = customersAmtSpent.filter(x => {
    x._2 > 5000
  })
  
  val doubleAmount = spentGrt5000.map(x => (x._1,x._2*2))
  
//  val topAmtSpenders = doubleAmount.sortByKey(false)
  
//  doubleAmount.collect().foreach(println)
  
//  println(doubleAmount.count())
  
  scala.io.StdIn.readLine()
  
//  topAmtSpenders.saveAsTextFile("/Users/abc/Desktop/topSpenders/")
}