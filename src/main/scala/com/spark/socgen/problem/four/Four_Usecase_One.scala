package com.spark.socgen.problem.four

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.{col, desc, lag, lead, row_number, when}

object Four_Usecase_One {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("Disney's Analysis").master("local").getOrCreate()
    val transformedDF  = runJob(spark, "src/resources/program4/disney_data.csv")
    println("Final Output========================>")
    transformedDF.show()
  }

  def runJob(spark: SparkSession, path:String): DataFrame ={
    val sensorData = spark.read.option("header", "true").csv(path)
    sensorData.show()
    import spark.implicits._
    val win = Window.orderBy(col("Sensor"))
    //val indexedData = sensorData.withColumnRenamed("timestamp", "start_timestamp").withColumn("row_num", monotonically_increasing_id()+1) //This wont give exact row number
    val indexedData = sensorData
      .withColumnRenamed("timestamp", "start_timestamp")
      .withColumn("row_num", row_number().over(win))
    indexedData.show()

    val w = Window.orderBy(col("row_num"))
    val preFinalDF = indexedData
      .withColumn("end_timestamp",
        when($"data" =!= lead($"data", 1).over(w),lead($"start_timestamp", 1).over(w))
          .otherwise(when(col("data").equalTo(lag($"data", 1).over(w)),"null")))
    preFinalDF.show()

    val lastrecord = preFinalDF.orderBy(desc("row_num")).limit(1)  //if we want to keep last record with null value
    val finalDF = preFinalDF
      .filter($"end_timestamp" =!= "null").union(lastrecord)
      .drop("row_num")
    //finalDF.show()
    //preFinalDF.filter($"end_timestamp" =!= "null").show() //if we dont want any null records
    finalDF
  }
}
