# WordCount
# Determine the most popular words in a given text file using Python and SQL.

filePath = "dbfs:/databricks-datasets/Rdatasets/README.md" # path in Databricks File System
lines = sc.textFile(filePath) # read the file into the cluster
lines.take(10) # display first 10 lines in the file
numPartitions = lines.getNumPartitions() # get the number of partitions
print "Number of partitions storing the dataset = %d" % numPartitions
words = lines.flatMap(lambda x: x.split(' ')) # split each line into a list of words
words.take(10) # display the first 10 words
stopWords = ['','a','*','and','is','of','the','a'] # define the list of stop words
filteredWords = words.filter(lambda x: x.lower() not in stopWords) # filter the words
filteredWords.take(10) # display the first 10 filtered words
filteredWords.cache() # cache filtered dataset into memory across the cluster worker nodes
word1Tuples = filteredWords.map(lambda x: (x, 1)) # map the words into (word,1) tuples
word1Tuples.take(10) # display the (word,1) tuples
wordCountTuples = word1Tuples.reduceByKey(lambda x, y: x + y) # aggregate counts for each word
wordCountTuples.take(10) # display the first 10 (word,count) tuples
sortedWordCountTuples = wordCountTuples.top(10,key=lambda (x, y): y) # top 10 (word,count) tuples
for tuple in sortedWordCountTuples: # display the top 10 (word,count) tuples by count
  print str(tuple)
from pyspark.sql import Row # import the pyspark sql Row class
wordCountRows = wordCountTuples.map(lambda p: Row(word=p[0], count=int(p[1]))) # tuples -> Rows
wordCountTable = sqlContext.createDataFrame(wordCountRows) # create a table from Rows
wordCountTable.registerTempTable("word_count") # register a temp table for querying
# use SQL to query words with count >= 2
# %sql
# SELECT word, count
# FROM word_count
# HAVING count >= 2
