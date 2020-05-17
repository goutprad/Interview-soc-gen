package com.spark.socgen.util

import java.io.{BufferedReader, File, InputStreamReader}
import java.net.URI
import java.util.Properties

import com.scala.socgen.util.FileFormat
import org.apache.hadoop.fs.{FSDataInputStream, FileSystem, Path}
import org.apache.poi.ss.usermodel.{Sheet, WorkbookFactory}
import org.apache.spark.sql.{DataFrame, SparkSession}

object Utility {
  def readExcel(spark:SparkSession,filePath:String, sheetName:String, colNames:List[String]): DataFrame ={
    val excelDf = spark.read
      .format("com.crealytics.spark.excel")
      .option("dataAddress","\'"+sheetName+"\'!A1")
      .option("useHeader", "true")
      .option("addColorColumns", "False")
      .option("inferSchema", "false")
      .option("treatEmptyValuesAsNulls","false")
      .load(filePath)
    val columnDf = excelDf.select(colNames.head,colNames.tail:_*)
    columnDf
  }

  def readFile(spark:SparkSession, filePath:String,fileType:String,sep:String): DataFrame ={
    fileType.toLowerCase() match {
      case FileFormat.CSV =>
        spark.read.option("header",true).option("sep",sep).csv(filePath)
      case FileFormat.Text =>
        spark.read.text(filePath)
    }

  }

  def propertyFileLoader(propPath:String,spark:SparkSession): Properties ={
    val prop = new Properties()
    val inputStream = readFileFromHDFS(propPath,spark)
    prop.load(inputStream)
    prop
  }

  def readFileFromHDFS(filePath:String,spark:SparkSession): FSDataInputStream ={
    val fs = FileSystem.get(new URI(filePath), spark.sessionState.newHadoopConf())
    fs.open(new Path(filePath))
  }

  def readExcelFromHDFS(spark:SparkSession, filePath:String, sheetName:String): Sheet ={
    val inputStream = readFileFromHDFS(filePath, spark: SparkSession)
    val workbook = WorkbookFactory.create(inputStream)
    //val buffered = new BufferedReader(new InputStreamReader(inputStream))
    //val result = Iterator.continually(buffered.readLine()).takeWhile(_ != null).toList
    val sheet = workbook.getSheet(sheetName)
    sheet
  }

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

}
