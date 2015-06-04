println("The sum of 1 and 1 is %s".format(1+1))

import scala.math._
val x = min(1, 10)

import java.util.HashMap
val map = new HashMap[String, Int]()
map.put("a", 1)
map.put("b", 2)
map.put("c", 3)
map.put("d", 4)
map.put("e", 5)

// The variable 'sc' access a Spark Context to run your Spark programs.
// wordcount(wordcount) < 100
val words = sc.parallelize(Array("hello", "world", "goodbye", "hello"))
val wordcounts = words.map(s => (s, 1)).reduceByKey(_ + _).collect()
words.count()

val numbers = sc.parallelize(Array(2,3,5,7))
val median = numbers.reduce(_ + _) / numbers.count()

displayHTML("<h3 style=\"color:blue\">Blue Text</h3>")

// SQL within Scala Notebooks
// %sql show databases
sqlContext.sql("show tables").take(5)

// Dataframes - a distributed collection of data organized into named columns
case class MyCaseClass(key: String, group: String, value: Int)
val dataframe = sc.parallelize(Array(MyCaseClass("f", "consonants", 1),
   MyCaseClass("g", "consonants", 2),
   MyCaseClass("h", "consonants", 3),
   MyCaseClass("i", "vowels", 4),
   MyCaseClass("j", "consonants", 5))
 ).toDF()
display(dataframe)
dataframe.registerTempTable("ScalaTempTable")
// %sql describe ScalaTempTable
// %sql select * from ScalaTempTable
sqlContext.sql("DROP TABLE IF EXISTS ScalaTestTable")
dataframe.saveAsTable("ScalaTestTable")

sqlContext.udf.register("repeatScala", (s: String) => s + s)
// %sql SELECT repeatScala(key) FROM ScalaTestTable;

sqlContext.udf.register("mod3Scala", (s: Int) => s%3)
// %sql select mod3Scala(value), value from ScalaTestTable;
