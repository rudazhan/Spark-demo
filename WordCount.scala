// WordCount
// Determine the most popular words in a given text file using Scala and SQL.

val filePath = "dbfs:/databricks-datasets/Rdatasets/README.md" // path in Databricks File System
val lines = sc.textFile(filePath) // read the file into the cluster
lines.take(10).mkString("\n") // display first 10 lines in the file
val numPartitions = lines.partitions.length // get the number of partitions
println(s"Number of partitions storing the dataset = $numPartitions")
val words = lines.flatMap(x => x.split(' ')) // split each line into a list of words
words.take(10).mkString("\n") // display the first 10 words
val stopWords = Seq("","a","*","and","is","of","the","a") // define the list of stop words
val filteredWords = words.filter(x => !stopWords.contains(x.toLowerCase())) // filter the words
filteredWords.take(10).mkString("\n") // display the first 10 filtered words
filteredWords.cache() // cache filtered dataset into memory across the cluster worker nodes
val word1Tuples = filteredWords.map(x => (x, 1)) // map the words into (word,1) tuples
word1Tuples.take(10).mkString("\n") // display the (word,1) tuples
val wordCountTuples = word1Tuples.reduceByKey{case (x, y) => x + y} // aggregate word counts
wordCountTuples.take(10).mkString("\n") // display the first 10 (word,count) tuples
val sortedWordCountTuples =
  wordCountTuples.top(10)(Ordering.by(tuple => tuple._2)).mkString("\n") // top 10 (word,count)
case class WordCount(word: String, count: Int) // create a case class to name the tuple elements
val wordCountRows = wordCountTuples.map(x => WordCount(x._1,x._2)) // tuples -> WordCount
val wordCountTable =
  wordCountRows.toDF().registerTempTable("word_count") // register a temp table for querying
// %sql
// SELECT word, count
// FROM word_count
// HAVING count >= 2 --use SQL to query words with count >= 2
