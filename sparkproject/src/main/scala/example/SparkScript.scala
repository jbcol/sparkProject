package example

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._
import java.io._


object SparkScript {
  def main(args: Array[String]): Unit = {
    println("coucou");

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


    val df = spark.read.option("header",true).option("delimiter", ";").schema(mySchema).csv("/home/col/Téléchargements/vacsi-v-dep-2023-07-13-15h51.csv");
    df.printSchema();
    df.show();

    spark.stop();
  }
}