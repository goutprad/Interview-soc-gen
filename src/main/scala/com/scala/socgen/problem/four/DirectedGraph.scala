package com.scala.socgen.problem.four

import scala.collection.mutable
import scala.collection.mutable.ListBuffer
case class Vertex(source:String,dest:String)
object DirectedGraph {
  var adjcencyList = mutable.LinkedHashMap[String, ListBuffer[String]]()
  var indexes = mutable.LinkedHashMap[String, Int]()
  var index = -1
  class Graph(list: List[String]){
    for(i <- 0 until list.size){
      val ver = list(i)
      val ll = ListBuffer[String]()
      adjcencyList += (ver -> ll)
      index = index +1
      indexes +=  (ver -> index)
    }
  }
  def addEdge(source:String, dest:String): Unit = {
    var list = ListBuffer[String]()
    list = adjcencyList(source)
    list += dest
    adjcencyList.->(source,dest)
  }

  def printGraph(): Unit ={
    val set = adjcencyList.keySet
    val it = set.iterator
    while(it.hasNext){
      val vert = it.next()
      print("Vertex ["+vert+"] is connected to: ")
      val ll = adjcencyList(vert)
      for(i <- 0 until ll.size){
        print("["+ll(i)+"] ")
      }
      println()
    }
  }

  def main(args: Array[String]): Unit = {
    val vertices = ListBuffer[String]()
    vertices += "Societe_Generale"
    vertices += "Credit Agricole"
    vertices += "UBS"
    vertices += "RBS"
    vertices += "BNP Paribas"
    vertices += "HSBC"
    vertices += "Santander"
    vertices += "Boursorama"
    vertices += "Deutsche"
    val gr = new Graph(vertices.toList)
    addEdge("Societe_Generale","UBS")
    addEdge("Societe_Generale","Credit Agricole")
    addEdge("Credit Agricole","HSBC")
    addEdge("Credit Agricole","BNP Paribas")
    addEdge("Credit Agricole","Boursorama")
    addEdge("HSBC","Santander")
    addEdge("BNP Paribas","Boursorama")
    addEdge("UBS","RBS")
    addEdge("RBS","Deutsche")
    printGraph()
  }

}
