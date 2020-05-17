package com.scala.socgen.problem.three

import com.scala.socgen.util.Util

import scala.collection.mutable.ListBuffer

case class HarvestData(year:String, harvest:String)
case class Report(Year:String, world_product_harvest:String,usa_product_contribution_percentage:String)
object Question_one {
  def main(args: Array[String]): Unit = {
  val reportFor = List("Barley","Beef","Sorghum")
  //"Corn","Cotton","Pork","Poultry","Rice","Soybeans","Soybean meal","Soybean oil","Wheat"
      for(i<-0 until reportFor.size){
        val out = generateReport("src/resources/program3/InternationalBaseline2019-Final.xlsx",reportFor(i))
        out.map(x=>println(x.Year+"\t"+x.world_product_harvest+"\t"+x.world_product_harvest))
        println()
      }
  }

  def generateReport(Excelpath:String,SheetName:String):  ListBuffer[Report] = {
    val input = Util.readExcel(Excelpath, SheetName)
    val inputData = Util.getExcelData(input)
    val reg = "(?=USA)(?s)(.*)".r
    val USAData = reg.findAllIn(inputData.mkString).toList.mkString.split("WORLD")(0)
    val reg2 = "(\\d.+)+.*".r
    val usa_req_Data = reg2.findAllIn(USAData)
    //println(usa_req_Data.toList.drop(1))
    val usaHarvestList = ListBuffer[HarvestData]()
    usa_req_Data.toList.drop(1).map(x => {
      val d = x.split("\\|")
      usaHarvestList += HarvestData(d(0), d(1))
    })
    val worldData = reg.findAllIn(inputData.mkString).toList.mkString.split("WORLD")(1)
    val world_req_Data = reg2.findAllIn(worldData)
    val worldHarvestList = ListBuffer[HarvestData]()
    world_req_Data.toList.drop(1).dropRight(1).map(x => {
      val d = x.split("\\|")
      worldHarvestList += HarvestData(d(0), d(1))
    })

    val report = ListBuffer[Report]()
    for (i <- 1 until usaHarvestList.size) {

     val percentage = if(usaHarvestList(i).harvest.matches("""\d.+""")) { //some records have "--"
        val perce = ((usaHarvestList(i).harvest.split("\\.")(0).toDouble / worldHarvestList(i).harvest.split("\\.")(0).toDouble) * 100)
        f"$perce%.1f%%"
      } else {
        "Invalid Data!"
      }
        report += Report(worldHarvestList(i).year, worldHarvestList(i).harvest, percentage)
      }
    report
    }
}
