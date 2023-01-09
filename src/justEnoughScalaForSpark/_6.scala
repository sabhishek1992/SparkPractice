package justEnoughScalaForSpark


//case keyword? It tells the compiler to do several useful things
//for us, eliminating a lot of boilerplate code that we would have to write for ourselves with other
//languages, especially Java:


object _6 extends App{
case class IIRecord(
		word: String,
		total_count: Int = 0,
		locations: Array[String] = Array.empty,
		counts: Array[Int] = Array.empty) {
	/**
	 * Different than our CSV output above, but see toCSV.
	 * Array.toString is useless, so format these ourselves.
	 */
	override def toString: String =
			s"""IIRecord($word, $total_count, $locStr, $cntStr)"""

			// "[_]" means we don't care what type of elements; we're just
			// calling toString on them!
			private def toArrayString(array: Array[_], delim: String = ","): String =
			array.mkString("[", delim, "]") // i.e., "[a,b,c]"


			private def locStr = toArrayString(locations)
			private def cntStr = toArrayString(counts)

			/** CSV-formatted string, but use [a,b,c] for the arrays */
			def toCSV: String =
			s"$word,$total_count,$locStr,$cntStr"

			/** Return a JSON-formatted string for the instance. */
			def toJSONString: String =
			s"""{
			| "word":       "$word",
			| "total_count": $total_count,
			| "locations":   ${toJSONArrayString(locations)},
			| "counts":      ${toArrayString(counts, ", ")}
			|}
			|""".stripMargin

			private def toJSONArrayString(array: Array[String]): String =
			toArrayString(array.map(quote), ", ")

			private def quote(word: String): String = "\"" + word + "\""

}

  val hello1 = new IIRecord("hello1")
  val hello2 = IIRecord("hello2",2) // The factory method 'apply' is called.
  val hello3 = IIRecord.apply("hello3") 
  
//whenever you put an argument list after any instance,
//including these objects , as in the hello2 case, Scala looks for an apply method
//to call. The arguments have to match the argument list for apply (number of arguments,
//types of arguments, accounting for default argument values, etc.)
  
  
  //===================
  //word stemming
  //===================
  // Scala allows object and class names to start with a lower case letter.
  object stem {
	  def apply(word: String): String = word.replaceFirst("s$", "") // insert real implementation!
}
println(stem("dog"))//calling a function or method named stem
println(stem("dogs"))
  
  //we can use our custom case classes in pattern matching expressions
  
  
/*Seq(IIRecord("hello"),IIRecord("world")).map {
  
    case IIRecord(word) => s"$word with no occurrences."
    case IIRecord(word, cnt, locs, cnts) =>
      s"$word occurs $cnt times: ${locs.zip(cnts).mkString(", ")}"
}*/
  
  
  
  
  
  
  
  
  
  
  
  
}