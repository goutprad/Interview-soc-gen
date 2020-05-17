package com.spark.socgen.problem.three

import com.spark.socgen.util.Utility
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions.{udf,lit}

/**
 * Problem - 3
 * Question -2
 */
object Question_Two {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("Harvest Percentage").master("local").getOrCreate()
    val reportFor = List("Barley","Beef","Sorghum")
    //"Corn","Cotton","Pork","Poultry","Rice","Soybeans","Soybean meal","Soybean oil","Wheat"  //to run for all, need to adjust regex, if time permits will do it
    for(i<- 0 until reportFor.size) {
      val report = runJob(spark, "src/resources/program3/InternationalBaseline2019-Final.xlsx", reportFor(i))
      report.show() //year|world_Barley_harvest|usa_Barley_contribution_percentage
    }
  }

  /**
   * udf - to find contribution
   *
   * @return String
   */
  def findContribution = (usaaHarvestInYear:String, worldHarvestInYear:String) =>
    if(usaaHarvestInYear.matches("""\d.+""") && worldHarvestInYear.matches("""\d.+""")) {
      val perce = (usaaHarvestInYear.split("\\.")(0).toDouble / worldHarvestInYear.split("\\.")(0).toDouble) * 100
      f"$perce%.1f%%"
    } else {
      "Invalid Data!"
    }

  def runJob(spark:SparkSession, filePath:String, sheetName:String): DataFrame ={
    val sheet = Utility.readExcelFromHDFS(spark, filePath,sheetName)
    val data = Utility.getExcelData(sheet)

    val reg = "(?=USA)(?s)(.*)".r //get Usa and World Data
    val reg2 = "(\\d.+)+.*".r //get only columns
    val USAData = reg.findAllIn(data.mkString).toList.mkString.split("WORLD")(0)
    val usa_req_Data = reg2.findAllIn(USAData)
    import spark.implicits._
    val usaDF = usa_req_Data.toList.drop(1).map(x => {
      val d = x.split("\\|")
      (d(0), d(1))
    }).toDF("year","usa_"+sheetName+"_harvest")

    val worldData = reg.findAllIn(data.mkString).toList.mkString.split("WORLD")(1)
    val world_req_Data = reg2.findAllIn(worldData)
    val worldDF = world_req_Data.toList.drop(1).dropRight(1).map(x => {
      val d = x.split("\\|")
      (d(0), d(1))
    }).toDF("year","world_"+sheetName+"_harvest")

    val contribution = udf(findContribution)
    worldDF.join(usaDF,Seq("year")).withColumn("usa_"+sheetName+"_contribution_percentage",contribution(usaDF("usa_"+sheetName+"_harvest"),worldDF("world_"+sheetName+"_harvest")))
      .select("year","world_"+sheetName+"_harvest","usa_"+sheetName+"_contribution_percentage")
  }

}
