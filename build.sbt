name := "SparkMovieRatings"

organization := "com.newday"

version := "1.0"

scalaVersion := "2.10.7"

val sparkVersion = "1.6.0"

libraryDependencies += "org.apache.spark" % "spark-core_2.10" % sparkVersion

libraryDependencies += "org.apache.spark" % "spark-sql_2.10" % sparkVersion

libraryDependencies += "org.apache.spark" %% "spark-hive" % sparkVersion

libraryDependencies += "com.typesafe.scala-logging" % "scala-logging-slf4j_2.10" % "2.1.2"

libraryDependencies += "com.holdenkarau" %% "spark-testing-base" % "1.6.3_0.8.0" % "test"

libraryDependencies += "org.scalatest" %% "scalatest" % "3.0.4" % "test"


    