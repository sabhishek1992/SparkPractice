object Worksheet {;import org.scalaide.worksheet.runtime.library.WorksheetSupport._; def main(args: Array[String])=$execute{;$skip(62); 
  println("Welcome to the Scala worksheet");$skip(30); 
  
  val a = Array(1,2,3,4,5);System.out.println("""a  : Array[Int] = """ + $show(a ));$skip(27); 
  println(a.mkString("|"))}
  
}
