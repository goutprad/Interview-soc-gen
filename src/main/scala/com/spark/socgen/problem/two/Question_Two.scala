package com.spark.socgen.problem.two

import com.spark.socgen.util.Utility
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, concat_ws}

object Question_Two {
  /**
   * Main Method
   *
   * @param args
   */
  def main(args: Array[String]): Unit = {
      System.setProperty("hadoop.home.dir","src\\resources\\winutils.exe")
      val spark = SparkSession.builder().appName("Adhar Auth").master("local").getOrCreate()
      runJob(spark)
    }

    /**
     * Problem -2
     * Question - 2 solution
     *
     * @param spark
     *
     * @observation -
     *             - As we have already structure data, Then we should not go back and convert to text or fixed width file and read.
     *             - We can achieve the same this but that need more developer efforts and maintaince will be bit difficult
     *             - We need to have separate schema and need to apply on top of data.
     *             - Splitting huge amount of column bit difficult and performance goes down
     *
     *             - Approach: if we have more columns to split - We can use SelectExpr(), inside this we have to pass required split statement as like sql statement
     *             else by using map and split we can proceed and later we can apply case class to it. as we have more columns maintaining this much case class is not recommended
     *
     *
     */
    def runJob(spark:SparkSession): Unit ={
      val prop = Utility.propertyFileLoader("src/resources/config.properties",spark)

      //val adharDf = Utility.readFile(spark,prop.getProperty("adharDataPath"),"csv",",")

      //Read as DataSet[String]
      val adharDf = spark.read.textFile(prop.getProperty("adharDataPath"))
      val header = adharDf.first()
      val adharData = adharDf.filter(x=> x != header)

      //if we need to consider all columns then dynamic way to go
      //adharDf.selectExpr("""split(",")(0).as("auth_code"),split(",")(0).as("auth_code")""")

      import spark.implicits._
      val requiredDF = adharData.map(x=> {
        val colArr = x.split(",")
        (colArr(3), colArr(2), colArr(128))
      }).toDF("sa", "aua","res_state_name").na.drop("any")
        .filter(x => x.getAs[String]("sa").matches("""\d+"""))
          .where(col("aua").gt(650000))
          .where(col("res_state_name") =!= "Delhi")
      requiredDF.show()
    }
}
