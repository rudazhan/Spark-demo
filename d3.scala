// Change these colors to your favorites to change the D3 visualization.
val colorsRDD = sc.parallelize(Array((197,27,125), (222,119,174), (241,182,218), (253,244,239), (247,247,247), (230,245,208), (184,225,134), (127,188,65), (77,146,33)))
val colors = colorsRDD.collect()

// Display HTML within a Spark Python/Scala notebook
displayHTML(sc.wholeTextFiles("d3.html"))
