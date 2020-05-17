package com.scala.socgen.util

import java.io.{BufferedReader, File, FileInputStream, FileReader}
import java.util.Scanner

import org.apache.poi.ss.usermodel.{Sheet, WorkbookFactory}

import scala.collection.mutable.ListBuffer


object Util {
  /**
   * readExcel
   *
   * @param path
   * @param sheetName
   * @return
   */
  def readExcel(path:String, sheetName:String): Sheet = {
    val f = new File(path)
    val workbook = WorkbookFactory.create(f)
    val sheet = workbook.getSheet(sheetName)
    sheet
  }

  /**
   * getExcelData
   *
   * @param sheet
   * @return
   */
  def getExcelData(sheet:Sheet): collection.mutable.ListBuffer[String] = {
    val excelList =  collection.mutable.ListBuffer[String]()
    val rowIt = sheet.iterator()
    while(rowIt.hasNext){
      val colIt = rowIt.next().cellIterator()
      var str = ""
      while(colIt.hasNext){
        str += colIt.next().toString +"|"
      }
      excelList += str +"\n"
    }
    excelList
  }

  /*def readInput(Path:String): ListBuffer[(String,String,String)] = {
    val dataList = ListBuffer[(String, String, String)]()
    val br = new BufferedReader(new FileReader(Path))
    val line = br.readLine()
    while(line != null){
      val colArr = line.split(",")
      dataList += (Tuple3(colArr(3),colArr(2),colArr(128)))
    }
    br.close()
    dataList
  }*/

  def readInput(Path:String): ListBuffer[(String,String,String)] = {
    val dataList = ListBuffer[(String, String, String)]()
    val inputStrm = new FileInputStream(Path)
    val scanner = new Scanner(inputStrm,"UTF-8")
    while(scanner.hasNextLine){
      val line = scanner.nextLine().split(",")
      dataList += Tuple3(line(3), line(2),line(128))
    }
    dataList
  }
}
