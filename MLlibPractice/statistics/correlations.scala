import org.apache.spark.SparkContext
import org.apache.spark.mllib.linalg._
import org.apache.spark.mllib.stat.Statistics
import org.apache.spark.rdd.RDD


val data: RDD[Vector] = sc.textFile("../data.txt").map(s => Vectors.dense(s.split(',').map(_.toDouble)))
data.collect() // note that each Vector is a row and not a column

// calculate the correlation matrix using Pearson's method. Use "spearman" for Spearman's method.
// If a method is not specified, Pearson's method will be used by default. 
val correlMatrix: Matrix = Statistics.corr(data, "pearson")