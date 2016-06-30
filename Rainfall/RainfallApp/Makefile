default: compile run
compile:
	sbt package
run: ./target/scala-2.11/rainfall-project_2.11-1.0.jar 
	spark-submit --class DaysOfRainfall --master local[8] ./target/scala-2.11/rainfall-project_2.11-1.0.jar 100
clean:
	rm -rf target project

