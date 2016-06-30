import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.spark.sql.SQLContext

import scala.collection.mutable.ArrayBuffer
import org.apache.spark.sql.types.{StructType,StructField,DoubleType};
import org.apache.spark.sql.Row;
import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.regression.LinearRegression
import org.apache.spark.ml.evaluation.RegressionEvaluator


object DaysOfRainfall {

	def main(args: Array[String]) {
        val conf = new SparkConf().setAppName("Simple Application")
        val sc = new SparkContext(conf)
    	val sqlContext = new SQLContext(sc)

    	val mean_temp_RDD = sc.textFile("./data/24_hour_temp_mean.csv").flatMap(flat_map_mean_temp)
		val rainy_days_RDD = sc.textFile("./data/num_of_rainy_days.txt").map(map_monthly_average)
		val rainfall_one_day_max_RDD = sc.textFile("./data/rainfall_one_day_max.txt").map(map_monthly_average)
		val daily_sunshine_average_RDD = sc.textFile("./data/sunshine_daily_mean.txt").map(map_monthly_average)
		val max_temp_mean_RDD = sc.textFile("./data/temp_mean_max.txt").map(map_monthly_average)
		val min_temp_mean_RDD = sc.textFile("./data/temp_mean_min.txt").map(map_monthly_average)
		val total_rainfall_RDD = sc.textFile("./data/total_monthly_rainfall.txt").map(map_monthly_average)

		// mean_temp_RDD.collect()
		// rainy_days_RDD.collect()
		// rainfall_one_day_max_RDD.collect()
		// daily_sunshine_average_RDD.collect()
		// max_temp_mean_RDD.collect()
		// min_temp_mean_RDD.collect()
		// total_rainfall_RDD.collect()

		val bad_RDD =  rainy_days_RDD.join(mean_temp_RDD).join(rainfall_one_day_max_RDD).join(daily_sunshine_average_RDD).join(max_temp_mean_RDD).join(min_temp_mean_RDD).join(total_rainfall_RDD)

		val collected_data_RDD = bad_RDD.map(fix_bad)
		// collected_data_RDD.collect()

		val schemaString = "month days_of_rain mean_temp rainfall_one_day_max daily_sunshine_average max_temp_mean min_temp_mean total_rainfall"
		val schema = StructType(schemaString.split(" ").map(fieldName => StructField(fieldName, DoubleType, true)))
		val cd_row_RDD = collected_data_RDD.map(p => Row(p._1,p._2,p._3,p._4,p._5,p._6,p._7,p._8))
		val df = sqlContext.createDataFrame(cd_row_RDD, schema)
		// df.show()

		val reduced_numeric_cols = schemaString.split(' ').filter(! _.contains("days_of_rain"))
		val assembler = new VectorAssembler().setInputCols(reduced_numeric_cols).setOutputCol("features")
		val classifier = new LinearRegression().setLabelCol("days_of_rain").setFeaturesCol("features")
		val pipeline = new Pipeline().setStages(Array(assembler, classifier))

		val Array(training_data, test_data) = df.randomSplit(Array(0.9, 0.1))
		val model = pipeline.fit(training_data)
		val predictions = model.transform(test_data)
		predictions.select("prediction", "days_of_rain", "features").show()

		val evaluator = new RegressionEvaluator().setLabelCol("days_of_rain").setPredictionCol("prediction").setMetricName("rmse")
		val rmse = evaluator.evaluate(predictions)
		println("Root Mean Squared Error (RMSE) on test data = " + rmse)
	}

	def flat_map_mean_temp(line: String) : ArrayBuffer[(Double, Double)] = {
	    val data = line.split(',')
	    val to_return = new ArrayBuffer[(Double, Double)]

	    for ((d, month) <- data.zipWithIndex)
	        if (month > 0)
	            to_return += Tuple2(data(0).toDouble + month.toDouble/100, d.toDouble)
	    
	    to_return
	}

	def map_monthly_average(line: String) : Tuple2[Double, Double] = {
		val year_and_other = line.split('M')
		val month_and_empty_and_rainy_days = year_and_other(1).split('^')
		val year_month = year_and_other(0).toInt + month_and_empty_and_rainy_days(0).toDouble/100
		val rainy_days = month_and_empty_and_rainy_days(2).toDouble

		Tuple2(year_month, rainy_days)
	}

	def fix_bad(data: (Double, ((((((Double, Double), Double), Double), Double), Double), Double))): Tuple8[Double, Double, Double, Double, Double, Double, Double, Double] = {
		(	data._1*100%100, 
		    data._2._1._1._1._1._1._1, 
		    data._2._1._1._1._1._1._2, 
		    data._2._1._1._1._1._2, 
		    data._2._1._1._1._2,
		    data._2._1._1._2, 
		    data._2._1._2, 
		    data._2._2	)
	}
}
