sc.textFile("text.txt").flatMap(line => line.split(" ")).map(word => (word, 1)).reduceByKey(_+_).saveAsTextFile("output")
