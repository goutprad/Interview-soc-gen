package com.scala.socgen.problem.one

import com.scala.socgen.util.Util

import collection.JavaConversions._
import scala.collection.mutable.ListBuffer

case class PovertyEstimate(FIPStxt:String,Stabr:String,Area_name:String,Urban_Influence_Code_2003:Int,Rural_urban_Continuum_Code_2013:Int,POV017_2018:Double)
case class StatesName(CapitalName:String,PostalAbbreviation:String)
case class Report(State:String,Area_name:String,Urban_Influence_Code_2003:Int,Rural_urban_Continuum_Code_2013:Int,Pov_elder_than17_2018:String)

/**
 * Problem - 1
 * Question - 1
 */
object Question_One {
  def main(args: Array[String]): Unit = {
    val stateSheet = Util.readExcel("src\\resources\\program1\\StatesName.xlsx", "Sheet2")
    val povertySheet = Util.readExcel("src\\resources\\program1\\PovertyEstimates.xls", "Poverty Data 2018")

    //collect poverty data
    val povertyEstimateList = ListBuffer[PovertyEstimate]()
    Util.getExcelData(povertySheet).mkString.split("CI90UB04P_2018\\|")(1).split("\n").toList.drop(1)
      .map(x=>{
        val d = x.split("\\|")
        List(d(0),d(1),d(2),d(4),d(5),d(13))
      }).filterNot(x=>x.contains("")).map(x=>{
      povertyEstimateList.add(PovertyEstimate(x.get(0),x.get(1),x.get(2),x.get(3).split("\\.")(0).toInt,x.get(4).split("\\.")(0).toInt,x.get(5).split("\\.")(0).toLong))
    })
    //collect states data
    val stateData = ListBuffer[StatesName]()
    val stateDetails = Util.getExcelData(stateSheet)
    stateDetails.mkString.split("\n").drop(1).toList.map(x=>{
      val d = x.split("\\|")
      stateData.add(StatesName(d(0),d(1)))
    })

    val capitalMap = collection.mutable.Map[String,String]()
    stateData.map(x=>capitalMap.put(x.PostalAbbreviation,x.CapitalName))
    println(povertyEstimateList.size)
    println(stateData.size)


    val rule3and4 = povertyEstimateList.filter(x=>x.Urban_Influence_Code_2003%2==1 && x.Rural_urban_Continuum_Code_2013%2==0)
    //Doubt on rule 4 so am not applying
    val totalCountElderThan17 = rule3and4.map(_.POV017_2018).sum

    val finalReport = ListBuffer[Report]()
    rule3and4.map(x=>{
      val percentage = (x.POV017_2018/totalCountElderThan17)*100
      finalReport.add(Report(capitalMap.getOrElse(x.Stabr,"Null"),x.Area_name+" "+x.Stabr,x.Urban_Influence_Code_2003,x.Rural_urban_Continuum_Code_2013,f"$percentage%.1f%%"))
    })
    println(finalReport.mkString("\n"))
  }
}
