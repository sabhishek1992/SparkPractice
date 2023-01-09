package justEnoughScalaForSpark

import justEnoughScalaForSpark._4.IIRecord1


//I said that defining secondary constructors is not very common. In part, it's because I
//used a convenient feature, the ability to define default values for arguments to methods,
//including the primary constructor. The default values mean that I can create instances
//without providing all the arguments explicitly, as long as there is a default value defined,
//and similarly for calling methods.

object _5 extends App{
case class IIRecord(
		word: String,
		total_count: Int = 0,
		locations: Array[String] = Array.empty,
		counts: Array[Int] = Array.empty) {
	/**
	 * Different than our CSV output above, but see toCSV.
	 * Array.toString is useless, so format these ourselves.
	 */
	override def toString: String = s"""IIRecord($word, $total_count, $locStr, $cntStr)"""

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

			private def quote(word: String): String = "\"" + word + "\""}

val hello = new IIRecord("hello")
val world = new IIRecord("world!", 3, Array("one", "two"), Array(1, 2))
println("\n`toString` output:")
println(hello)
println(world)
println("\n`toJSONString` output:")
println(hello.toJSONString)
println(world.toJSONString)
println("\n`toCSV` output:")
println(hello.toCSV)
println(world.toCSV)

}