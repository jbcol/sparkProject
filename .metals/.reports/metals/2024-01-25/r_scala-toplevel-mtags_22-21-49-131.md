error id: file://<WORKSPACE>/sparkproject/src/main/scala/example/SparkScript.scala:[1117..1123) in Input.VirtualFile("file://<WORKSPACE>/sparkproject/src/main/scala/example/SparkScript.scala", "package example

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._
import java.io._

def writeToFile(fileName:String, data:String) = {
  val writer = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(fileName)))
  writer.write(data)
  writer.close()
}

def getVaccin(df: org.apache.spark.sql.DataFrame, dep: Int, vaccin: Int, debut:String, fin:String, dose:Int) = {
  df.filter("dep = "+dep+" and vaccin = "+vaccin+" and (jour = '"+debut+"' or jour = '"+fin+"')").collect().map(x => x(12).asInstanceOf[Int]).reduce((x, y) => y-x)
}

def getSchemaComVaccin(df: org.apache.spark.sql.DataFrame, dep: Int, vaccin: Int, debut:String, fin:String, dose:Int) = {
  df.filter("dep = "+dep+" and vaccin = 1 and (jour = '2020-12-27' or jour = '2023-07-10')").collect().map(x => x(16).asInstanceOf[Int]).reduce((x, y) => y-x)
}

def getTotalVaccin(df: org.apache.spark.sql.DataFrame, dep: Int, jour:String, dose:Int) = {
  df.filter("dep = "+dep+" and jour = '"+jour+"'").collect().map(x => x(dose).asInstanceOf[Int]).reduce((x, y) => x+y)
}

def 

object SparkScript {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder.appName("SparkScript").getOrCreate();

    val mySchema = StructType(Array(
      StructField("dep", IntegerType, true),
      StructField("vaccin", IntegerType , true),
      StructField("jour", StringType, true),
      StructField("n_dose1", IntegerType, true),
      StructField("n_dose2", IntegerType , true),
      StructField("n_dose3", IntegerType , true),
      StructField("n_dose4", IntegerType , true),
      StructField("n_complet", IntegerType , true),
      StructField("n_rappel", IntegerType , true),
      StructField("n_2_rappel", IntegerType , true),
      StructField("n_rappel_biv", IntegerType , true),
      StructField("n_3_rappel", IntegerType , true),
      StructField("n_cum_dose1", IntegerType, true),
      StructField("n_cum_dose2", IntegerType , true),
      StructField("n_cum_dose3", IntegerType , true),
      StructField("n_cum_dose4", IntegerType , true),
      StructField("n_cum_complet", IntegerType , true),
      StructField("n_cum_rappel", IntegerType , true),
      StructField("n_cum_2_rappel", IntegerType , true),
      StructField("n_cum_rappel_biv", IntegerType , true),
      StructField("n_cum_3_rappel", IntegerType , true)
      ));


    val df = spark.read.option("header",true).option("delimiter", ";").schema(mySchema).csv("<HOME>/Téléchargements/vacsi-v-dep-2023-07-13-15h51.csv");
    df.printSchema();
    df.show();

    spark.stop();
  }
}")
file://<WORKSPACE>/sparkproject/src/main/scala/example/SparkScript.scala
file://<WORKSPACE>/sparkproject/src/main/scala/example/SparkScript.scala:28: error: expected identifier; obtained object
object SparkScript {
^
#### Short summary: 

expected identifier; obtained object