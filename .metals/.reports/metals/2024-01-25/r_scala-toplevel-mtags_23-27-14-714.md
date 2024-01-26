error id: file://<WORKSPACE>/sparkproject/src/main/scala/example/SparkScript.scala:[375..378) in Input.VirtualFile("file://<WORKSPACE>/sparkproject/src/main/scala/example/SparkScript.scala", "package example

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._
import java.io._


object SparkScript {
  def writeToFile1(fileName:String, data:String) = {
    val writer = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(fileName)))
    writer.write(data)
    writer.close()
  }

  def

  def getVaccin(df: org.apache.spark.sql.DataFrame, dep: Int, vaccin: Int, debut:String, fin:String, dose:Int) = {
    df.filter("dep = "+dep+" and vaccin = "+vaccin+" and (jour = '"+debut+"' or jour = '"+fin+"')").collect().map(x => x(dose).asInstanceOf[Int]).reduce((x, y) => y-x)
  }

  def getTotalVaccin(df: org.apache.spark.sql.DataFrame, dep: Int, jour:String, dose:Int) = {
    df.filter("dep = "+dep+" and jour = '"+jour+"'").collect().map(x => x(dose).asInstanceOf[Int]).reduce((x, y) => x+y)
  }

  def getSchemaComplet(df: org.apache.spark.sql.DataFrame, dep: Int, vaccin: Int, debut:String, fin:String) = {
    df.filter("dep = "+dep+" and vaccin = "+vaccin+" and (jour = '"+debut+"' or jour = '"+fin+"')").collect().map(x => x(16).asInstanceOf[Int]).reduce((x, y) => y-x)
  }

  def getSchemaCompletToutVaccin(df: org.apache.spark.sql.DataFrame, dep: Int, jour:String) = {
    df.filter("dep = "+dep+" and jour = '"+jour+"'").collect().map(x => x(16).asInstanceOf[Int]).reduce((x, y) => x+y)
  }

  def getSchemaCompletVaccin(df: org.apache.spark.sql.DataFrame, dep: Int, vaccin: Int, debut:String, fin:String) = {
    df.filter("dep = "+dep+" and vaccin = "+vaccin+" and (jour = '"+debut+"' or jour = '"+fin+"')").collect().map(x => x(17).asInstanceOf[Int]).reduce((x, y) => y-x)
  }

  def getSchemaCompletVaccin(df: org.apache.spark.sql.DataFrame, dep: Int, jour:String) = {
    df.filter("dep = "+dep+" and jour = '"+jour+"'").collect().map(x => x(17).asInstanceOf[Int]).reduce((x, y) => x+y)
  }


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


    val debut_periode = "2020-12-27"
    val fin_periode = "2023-07-10"

    val departements = Array("01","02","03","04","05","06","07","08","09","10","11","12","13","14","15",
    "16","17","18","19","21","22","23","24","25","26","27","28","29","2A","2B","30","31","32","33","34","35",
    "36","37","38","39","40","41","42","43","44","45","46","47","48","49","50","51","52","53","54","55",
    "56","57","58","59","60","61","62","63","64","65","66","67","68","69","70","71","72","73","74",
    "75","76","77","78","79","80","81","82","83","84","85","86","87","88","89","90","91","92",
    "93","94","95","971","972","973","974","976", "977", "978") 

    //try to write something in a file output.csv without using writeToFile function
    val file = new File("<WORKSPACE>/output.csv")
    val bw = new BufferedWriter(new FileWriter(file))
    bw.write("dep;vaccin;jour;n_dose1;n_dose2;n_dose3;n_dose4;n_complet;n_rappel;n_2_rappel;n_rappel_biv;n_3_rappel;n_cum_dose1;n_cum_dose2;n_cum_dose3;n_cum_dose4;n_cum_complet;n_cum_rappel;n_cum_2_rappel;n_cum_rappel_biv;n_cum_3_rappel\n")
    bw.close()


    val percentageDepVaccin0 = departements.map(x => {
      val percentage = getVaccin(df, x.toInt, 0, debut_periode, fin_periode, 1)/getTotalVaccin(df, x.toInt, fin_periode, 1).toDouble
    })

    //export to csv file
    val csv1 = departements.zip(percentageDepVaccin0).map(x => x._1+";"+x._2).mkString("\n")
    writeToFile("<WORKSPACE>/percentageDepVaccin0.csv", csv1)

    val percentageDepVaccin9 = departements.map(x => {
      val percentage = getVaccin(df, x.toInt, 1, debut_periode, fin_periode, 9)/getTotalVaccin(df, x.toInt, fin_periode, 9).toDouble
    })

    //export to csv file
    val csv9 = departements.zip(percentageDepVaccin9).map(x => x._1+";"+x._2).mkString("\n")
    writeToFile("<WORKSPACE>/percentageDepVaccin9.csv", csv9)

    spark.stop();
  }
}")
file://<WORKSPACE>/sparkproject/src/main/scala/example/SparkScript.scala
file://<WORKSPACE>/sparkproject/src/main/scala/example/SparkScript.scala:18: error: expected identifier; obtained def
  def getVaccin(df: org.apache.spark.sql.DataFrame, dep: Int, vaccin: Int, debut:String, fin:String, dose:Int) = {
  ^
#### Short summary: 

expected identifier; obtained def