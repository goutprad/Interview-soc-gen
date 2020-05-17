package com.scala.socgen.problem.two

import com.scala.socgen.util.Util

import scala.collection.mutable.ListBuffer

case class Adhar(sa:String,aua:Long,res_state_name:String)
object Question_One {
  def main(args: Array[String]): Unit = {
    val Data = ListBuffer[Adhar]()
    val adharData = Util.readInput("src/resources/program2/auth.csv")
   val filtered = adharData.drop(1)
     .map(x=>List(x._1,x._2,x._3))
     .filterNot(x=>x.contains(""))
     .map(x=>{
      Adhar(x(0),x(1).toLong,x(2))
    }).filter(x=>x.sa.matches("""\d+"""))
       .filter(x=>x.aua > 650000)
       .filter(x=>x.res_state_name != "Delhi")
       .map(x=>println(x.sa +"\t" +x.aua+"\t"+x.res_state_name))
  }
}
