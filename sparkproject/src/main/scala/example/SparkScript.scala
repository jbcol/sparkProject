package example

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._
import java.io._


object SparkScript {
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

  def getDiff(df: org.apache.spark.sql.DataFrame, dep: Int, vaccin: Int, jour:String):Int = {
    df.filter("dep = "+dep+" and vaccin = "+vaccin+" and jour = '"+jour+"'").collect().map(x => x(16).asInstanceOf[Int] - x(17).asInstanceOf[Int]).first
  }

  def getTotalDiff(df: org.apache.spark.sql.DataFrame, dep: Int, jour:String):Int = {
    df.filter("dep = "+dep+" and jour = '"+jour+"'").map(x => x(16).asInstanceOf[Int] - x(17).asInstanceOf[Int]).reduce((x, y) => x+y)
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
    val percentageDepVaccin1 = departements.map(x => getVaccin(df, x.toInt, 1, debut_periode, fin_periode, 12)/getTotalVaccin(df, x.toInt, fin_periode, 12).toDouble)
    val csv0 = departements.zip(percentageDepVaccin1).map(x => x._1+";"+x._2).mkString("\n")
    writeToFile("/run/media/col/Data/DATA/workspace/dataviz_websem/percentageVaccin1ByDep.csv", csv0)

    val percentageDepVaccin2 = departements.map(x => getVaccin(df, x.toInt, 2, debut_periode, fin_periode, 12)/getTotalVaccin(df, x.toInt, fin_periode, 12).toDouble)
    val csv1 = departements.zip(percentageDepVaccin2).map(x => x._1+";"+x._2).mkString("\n")
    writeToFile("/run/media/col/Data/DATA/workspace/dataviz_websem/percentageVaccin2ByDep.csv", csv1)
    
    ////////////////////////////////////////////////////////////////////////////
    val percentageSchemaComplet1 = departements.map(dep => getSchemaComplet(df, dep.toInt, 1, debut_periode, fin_periode)/getTotalSchemaComplet(df, dep.toInt, fin_periode).toDouble)
    val csv2 = departements.zip(percentageSchemaComplet1).map(x => x._1+";"+x._2).mkString("\n")
    writeToFile("/run/media/col/Data/DATA/workspace/dataviz_websem/percentageSchemaComplet1ByDep.csv", csv2)

    val percentageSchemaComplet2 = departements.map(dep => getSchemaComplet(df, dep.toInt, 2, debut_periode, fin_periode)/getTotalSchemaComplet(df, dep.toInt, fin_periode).toDouble)
    val csv3 = departements.zip(percentageSchemaComplet2).map(x => x._1+";"+x._2).mkString("\n")
    writeToFile("/run/media/col/Data/DATA/workspace/dataviz_websem/percentageSchemaComplet2ByDep.csv", csv3)
    
    ////////////////////////////////////////////////////////////////////////////
    val percentageRappel1 = departements.map(dep => getRappelVaccin(df, dep.toInt, 1, debut_periode, fin_periode)/getTotalRappelVaccin(df, dep.toInt, fin_periode).toDouble)
    val csv4 = departements.zip(percentageRappel1).map(x => x._1+";"+x._2).mkString("\n")
    writeToFile("/run/media/col/Data/DATA/workspace/dataviz_websem/percentageRappel1ByDep.csv", csv4)

    val percentageRappel2 = departements.map(dep => getRappelVaccin(df, dep.toInt, 2, debut_periode, fin_periode)/getTotalRappelVaccin(df, dep.toInt, fin_periode).toDouble)
    val csv5 = departements.zip(percentageRappel2).map(x => x._1+";"+x._2).mkString("\n")
    writeToFile("/run/media/col/Data/DATA/workspace/dataviz_websem/percentageRappel2ByDep.csv", csv5)

    ///////////////////////////////////////////////////

    val diff1 = departements.map(dep => 10*getDiff(df, dep.toInt, 1, fin_periode)/(getTotalDiff(df, dep.toInt, fin_periode).toDouble))
    val csv14 = departements.zip(diff1).map(x => x._1+";"+x._2).mkString("\n")
    writeToFile("/run/media/col/Data/DATA/workspace/dataviz_websem/diff1ByDep.csv", csv14)

    /////////////////////////////////////////////////////////////////////////////////
    //pourentages de vaccinés par département dans le temps
    val fin_periode1 = "2021-04-27"
    val fin_periode2 = "2021-08-27"
    val fin_periode3 = "2021-12-27"
    val fin_periode4 = "2022-04-27"
    val fin_periode5 = "2022-08-27"
    val fin_periode6 = "2022-12-27"
    val fin_periode7 = "2023-04-27"
    val fin_periode8 = "2023-07-10"

    val totVaccinedByDepPeriode1 = departements.map(x => getTotalVaccin(df, x.toInt, fin_periode1, 12).toDouble)
    val csv6 = departements.zip(totVaccinedByDepPeriode1).map(x => x._1+";"+x._2).mkString("\n")
    writeToFile("/run/media/col/Data/DATA/workspace/dataviz_websem/csvTotPopVaccinedTime/totVaccinedByDepPeriode1.csv", csv6)

    val totVaccinedByDepPeriode2 = departements.map(x => getTotalVaccin(df, x.toInt, fin_periode2, 12).toDouble)
    val csv7 = departements.zip(totVaccinedByDepPeriode2).map(x => x._1+";"+x._2).mkString("\n")
    writeToFile("/run/media/col/Data/DATA/workspace/dataviz_websem/csvTotPopVaccinedTime/totVaccinedByDepPeriode2.csv", csv7)

    val totVaccinedByDepPeriode3 = departements.map(x => getTotalVaccin(df, x.toInt, fin_periode3, 12).toDouble)
    val csv8 = departements.zip(totVaccinedByDepPeriode3).map(x => x._1+";"+x._2).mkString("\n")
    writeToFile("/run/media/col/Data/DATA/workspace/dataviz_websem/csvTotPopVaccinedTime/totVaccinedByDepPeriode3.csv", csv8)

    val totVaccinedByDepPeriode4 = departements.map(x => getTotalVaccin(df, x.toInt, fin_periode4, 12).toDouble)
    val csv9 = departements.zip(totVaccinedByDepPeriode4).map(x => x._1+";"+x._2).mkString("\n")
    writeToFile("/run/media/col/Data/DATA/workspace/dataviz_websem/csvTotPopVaccinedTime/totVaccinedByDepPeriode4.csv", csv9)

    val totVaccinedByDepPeriode5 = departements.map(x => getTotalVaccin(df, x.toInt, fin_periode5, 12).toDouble)
    val csv10 = departements.zip(totVaccinedByDepPeriode5).map(x => x._1+";"+x._2).mkString("\n")
    writeToFile("/run/media/col/Data/DATA/workspace/dataviz_websem/csvTotPopVaccinedTime/totVaccinedByDepPeriode5.csv", csv10)

    val totVaccinedByDepPeriode6 = departements.map(x => getTotalVaccin(df, x.toInt, fin_periode6, 12).toDouble)
    val csv11 = departements.zip(totVaccinedByDepPeriode6).map(x => x._1+";"+x._2).mkString("\n")
    writeToFile("/run/media/col/Data/DATA/workspace/dataviz_websem/csvTotPopVaccinedTime/totVaccinedByDepPeriode6.csv", csv11)

    val totVaccinedByDepPeriode7 = departements.map(x => getTotalVaccin(df, x.toInt, fin_periode7, 12).toDouble)
    val csv12 = departements.zip(totVaccinedByDepPeriode7).map(x => x._1+";"+x._2).mkString("\n")
    writeToFile("/run/media/col/Data/DATA/workspace/dataviz_websem/csvTotPopVaccinedTime/totVaccinedByDepPeriode7.csv", csv12)

    val totVaccinedByDepPeriode8 = departements.map(x => getTotalVaccin(df, x.toInt, fin_periode8, 12).toDouble)
    val csv13 = departements.zip(totVaccinedByDepPeriode8).map(x => x._1+";"+x._2).mkString("\n")
    writeToFile("/run/media/col/Data/DATA/workspace/dataviz_websem/csvTotPopVaccinedTime/totVaccinedByDepPeriode8.csv", csv13) */
    

    spark.stop();
  }
}