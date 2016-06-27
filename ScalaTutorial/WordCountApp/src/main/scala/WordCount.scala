/* WordCount.scala */
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf

object WordCount {
  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("Simple Application")
    val sc = new SparkContext(conf)
    val result = sc.textFile("../text.txt").flatMap(line => line.split(" ")).map(word => (word, 1)).reduceByKey(_+_).collect()
    result.foreach(println)
    }
}
