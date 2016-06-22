compile:
	hadoop com.sun.tools.javac.Main StudentDetector.java
prepare:
	jar cf sd.jar StudentDetector*.class
	hadoop fs -rm -r sample_student_scores
	hadoop fs -put sample_student_scores
run: compile prepare
	hadoop jar sd.jar StudentDetector sample_student_scores/input sample_student_scores/intermediate sample_student_scores/output
	hadoop fs -cat sample_student_scores/output/part-r-00000
verify:
	javac Verify.java
	java Verify
clean:
	rm *.class
	rm sd.jar
