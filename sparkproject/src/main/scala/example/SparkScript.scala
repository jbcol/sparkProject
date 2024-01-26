package example

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._
import java.io._


object SparkScript {
/*   def writeToFile1(fileName:String, data:String) = {
    val writer = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(fileName)))
    writer.write(data)
    writer.close()
  } */

  def writeToFile(fileName:String, data:String):Unit = {
    var file = new File(fileName)
    var bw = new BufferedWriter(new FileWriter(file))
    bw.write(data)
    bw.close()
  }

  def getVaccin(df: org.apache.spark.sql.DataFrame, dep: Int, vaccin: Int, debut:String, fin:String, dose:Int):Int = {
    df.filter("dep = "+dep+" and vaccin = "+vaccin+" and (jour = '"+debut+"' or jour = '"+fin+"')").collect().map(x => x(dose).asInstanceOf[Int]).reduce((x, y) => y-x)
  }

  def getTotalVaccin(df: org.apache.spark.sql.DataFrame, dep: Int, jour:String, dose:Int):Int = {
    df.filter("dep = "+dep+" and jour = '"+jour+"'").collect().map(x => x(dose).asInstanceOf[Int]).reduce((x, y) => x+y)
  }

  def getSchemaComplet(df: org.apache.spark.sql.DataFrame, dep: Int, vaccin: Int, debut:String, fin:String):Int = {
    df.filter("dep = "+dep+" and vaccin = "+vaccin+" and (jour = '"+debut+"' or jour = '"+fin+"')").collect().map(x => x(16).asInstanceOf[Int]).reduce((x, y) => y-x)
  }

  def getTotalSchemaComplet(df: org.apache.spark.sql.DataFrame, dep: Int, jour:String):Int = {
    df.filter("dep = "+dep+" and jour = '"+jour+"'").collect().map(x => x(16).asInstanceOf[Int]).reduce((x, y) => x+y)
  }

  def getRappelVaccin(df: org.apache.spark.sql.DataFrame, dep: Int, vaccin: Int, debut:String, fin:String):Int = {
    df.filter("dep = "+dep+" and vaccin = "+vaccin+" and (jour = '"+debut+"' or jour = '"+fin+"')").collect().map(x => x(17).asInstanceOf[Int]).reduce((x, y) => y-x)
  }

  def getTotalRappelVaccin(df: org.apache.spark.sql.DataFrame, dep: Int, jour:String):Int = {
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
    
    val df = spark.read.option("header",true).option("delimiter", ";").schema(mySchema).csv("/home/col/Téléchargements/vacsi-v-dep-2023-07-13-15h51.csv");

    val debut_periode = "2020-12-27"
    val fin_periode = "2023-07-10"

    val departements = Array("01","02","03","04","05","06","07","08","09","10","11","12","13","14","15",
    "16","17","18","19","21","22","23","24","25","26","27","28","29","30","31","32","33","34","35",
    "36","37","38","39","40","41","42","43","44","45","46","47","48","49","50","51","52","53","54","55",
    "56","57","58","59","60","61","62","63","64","65","66","67","68","69","70","71","72","73","74",
    "75","76","77","78","79","80","81","82","83","84","85","86","87","88","89","90","91","92",
    "93","94","95","971","972","973","974","976", "977", "978") 


    /////////////////////////////////////////////////////////////////////////////////
/*     val percentageDepVaccin1 = departements.map(x => getVaccin(df, x.toInt, 1, debut_periode, fin_periode, 12)/getTotalVaccin(df, x.toInt, fin_periode, 12).toDouble)
    val csv0 = departements.zip(percentageDepVaccin1).map(x => x._1+";"+x._2).mkString("\n")
    writeToFile("/run/media/col/Data/DATA/workspace/dataviz_websem/percentageVaccin1ByDep.csv", csv0)

    val percentageDepVaccin9 = departements.map(x => getVaccin(df, x.toInt, 9, debut_periode, fin_periode, 12)/getTotalVaccin(df, x.toInt, fin_periode, 12).toDouble)
    val csv1 = departements.zip(percentageDepVaccin9).map(x => x._1+";"+x._2).mkString("\n")
    writeToFile("/run/media/col/Data/DATA/workspace/dataviz_websem/percentageVaccin9ByDep.csv", csv1) */
    
    ////////////////////////////////////////////////////////////////////////////
    val percentageSchemaComplet1 = departements.map(dep => getSchemaComplet(df, dep.toInt, 1, debut_periode, fin_periode)/getTotalSchemaComplet(df, dep.toInt, fin_periode).toDouble)
    val csv2 = departements.zip(percentageSchemaComplet1).map(x => x._1+";"+x._2).mkString("\n")
    writeToFile("/run/media/col/Data/DATA/workspace/dataviz_websem/percentageSchemaComplet1ByDep.csv", csv2)

    val percentageSchemaComplet9 = departements.map(dep => getSchemaComplet(df, dep.toInt, 9, debut_periode, fin_periode)/getTotalSchemaComplet(df, dep.toInt, fin_periode).toDouble)
    val csv3 = departements.zip(percentageSchemaComplet9).map(x => x._1+";"+x._2).mkString("\n")
    writeToFile("/run/media/col/Data/DATA/workspace/dataviz_websem/percentageSchemaComplet9ByDep.csv", csv3)
    
    ////////////////////////////////////////////////////////////////////////////
    val percentageRappel1 = departements.map(dep => getRappelVaccin(df, dep.toInt, 1, debut_periode, fin_periode)/getTotalRappelVaccin(df, dep.toInt, fin_periode).toDouble)
    val csv4 = departements.zip(percentageRappel1).map(x => x._1+";"+x._2).mkString("\n")
    writeToFile("/run/media/col/Data/DATA/workspace/dataviz_websem/percentageRappel1ByDep.csv", csv4)

    val percentageRappel9 = departements.map(dep => getRappelVaccin(df, dep.toInt, 9, debut_periode, fin_periode)/getTotalRappelVaccin(df, dep.toInt, fin_periode).toDouble)
    val csv5 = departements.zip(percentageRappel9).map(x => x._1+";"+x._2).mkString("\n")
    writeToFile("/run/media/col/Data/DATA/workspace/dataviz_websem/percentageRappel9ByDep.csv", csv5)

    spark.stop();
  }
}