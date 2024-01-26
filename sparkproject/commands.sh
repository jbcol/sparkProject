#to maker the jar file
sbt package


SPARK_SUBMIT_EXECUTABLE_PATH="/home/col/Téléchargements/spark-3.5.0-bin-hadoop3/bin/spark-submit"
JAR_PATH="/run/media/col/Data/DATA/workspace/dataviz_websem/sparkproject/target/scala-2.12/sparkscript_2.12-1.0.jar"
$SPARK_SUBMIT_EXECUTABLE_PATH --class "example.SparkScript" --master local[4] --verbose $JAR_PATH