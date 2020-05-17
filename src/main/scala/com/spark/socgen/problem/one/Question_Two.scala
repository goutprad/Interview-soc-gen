package com.spark.socgen.problem.one

import com.spark.socgen.util.Utility
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col,concat_ws}

object Question_Two {
  def main(args: Array[String]): Unit = {
    System.setProperty("hadoop.home.dir","src\\resources\\winutils.exe")
    val spark = SparkSession.builder().appName("Poverty Estimate").master("local").getOrCreate()
    runJob(spark)
  }

  /**
   * Problem -1
   * Question - 2 solution
   *
   * @param spark
   */
  def runJob(spark:SparkSession): Unit ={
    val prop = Utility.propertyFileLoader("src/resources/config.properties",spark)

    val stateDet = prop.getProperty("StatesDataPath").split("\\|")
    val povertyDet = prop.getProperty("PovertyEstimateDataPath").split("\\|")
    val stateDF = Utility.readExcel(spark,stateDet(0),stateDet(1),stateDet(2).split(",").toList)
    val povertyDF = Utility.readExcel(spark,povertyDet(0),povertyDet(1),povertyDet(2).split(",").toList)


    import spark.implicits._
    val filteredDF = povertyDF.na.drop("any").filter($"Rural-urban_Continuum_Code_2013" % 2 === 0 && $"Urban_Influence_Code_2003" % 2 === 1)
    val joinDf = filteredDF.join(stateDF,filteredDF("Stabr") === stateDF("Postal Abbreviation"))
    joinDf
      //.withColumnRenamed("Rural-urban_Continuum_Code_2013","Rural_urban_Continuum_Code_2013")
      .withColumn("Area_Name", concat_ws(" ",col("Area_name"),col("Stabr")))
      //.withColumn("POV_elder_than17_2018",col()) //Rule - 5 need clarification
      .select(col("Capital Name").as("State"),col("Area_Name"),col("Urban_Influence_Code_2003"),col("Rural-urban_Continuum_Code_2013").as("Rural_urban_Continuum_Code_2013"), col("POV017_2018").as("POV_elder_than17_2018"))
      .show(false)
  }
}
