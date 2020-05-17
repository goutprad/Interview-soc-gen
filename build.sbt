name := "soc-gen"

version := "0.1"

scalaVersion := "2.11.12"

//for scala program
libraryDependencies += "org.apache.poi" % "poi" % "4.1.2"
libraryDependencies += "org.apache.poi" % "poi-ooxml" % "4.1.2"


//for Spark
libraryDependencies += "org.apache.spark" %% "spark-core" % "2.4.5"
libraryDependencies += "org.apache.spark" %% "spark-sql" % "2.4.5"
libraryDependencies += "com.crealytics" %% "spark-excel" % "0.12.2"




