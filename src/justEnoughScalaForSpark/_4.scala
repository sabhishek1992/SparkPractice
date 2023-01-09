package justEnoughScalaForSpark

object _4 extends App{
  
  val stuff = Seq(1, 3.14159, 2L, 4.4F, ("one", 1), (404F, "boo"), ((11, 12),21,31))

	stuff.foreach {
		  case i: Int => println(s"Found an Int: $i")
		  case l: Long => println(s"Found a Long: $l")
		  case f: Float => println(s"Found a Float: $f")
		  case d: Double => println(s"Found a Double: $d")
		  case (x1, x2) =>
		  println(s"Found a two-element tuple with elements of arbitrary type: ($x1,$x2)")
		  case ((x1a, x1b), _, x3) =>
		  println(s"Found a three-element tuple with 1st and 3th elements: (($x1a,$x1b),_,$x3)")
	
		  //default-clause, '_' can be anything even default. We avoid MatchError by this
		  case default => println(s"Found something else: $default")
}
  
  val (a, (b, (c1, c2), d)) = ("A", ("B", ("C1", "C2"), "D"))
  println(s" $a, $b, $c1, $c2, $d")
 
  
// Scala uses the same distinction between classes and instances that you find in Java.
// Classes are like templates used to create instances
  
  
  //When defining a class, the argument list after the class name is the argument list for the
  //primary constructor
  class IIRecord1(
		  word: String,
		  total_count: Int,
		  locations: Array[String],
		  counts: Array[Int]) {
    
  /** CSV formatted string, but use [a,b,c] for the arrays */
  
    override def toString: String = {
      val locStr = locations.mkString("[", ",", "]") // i.e., "[a,b,c]"
      val cntStr = counts.mkString("[", ",", "]") // i.e., "[1,2,3]"
      s"$word,$total_count,$locStr,$cntStr"
  }
    
  
//  I've been careful to use the word instance for things we create from classes. That's
//because Scala has built-in support for the Singleton Design Pattern, i.e., when we only
//want one instance of a class. We use the object keyword.
// For example, in Java, you define a class with a static void main(String[]
//arguments) method as your entry point into your program. In Scala, you use an
//object to hold main
    
    object MySparkJob{
      val greeting = "Good Night!"
      def main(args: Array[String]) = {
        println(greeting)
      }
    }
   
//    Just as for classes, the name of the object can be anything you want. There is no
//static keyword in Scala. Instead of adding static methods and fields to classes as
//in Java, you put them in objects instead, as here.
    
}

println(new IIRecord1("hello", 3, Array("one", "two"), Array(1, 2)))














}