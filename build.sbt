name := "SparkMovieRatings"

organization := "com.newday"

version := "1.0"

scalaVersion := "2.10.7"

val sparkVersion = "1.6.0"

libraryDependencies += "org.apache.spark" % "spark-core_2.10" % sparkVersion

libraryDependencies += "org.apache.spark" % "spark-sql_2.10" % sparkVersion

libraryDependencies += "org.apache.spark" %% "spark-hive" % sparkVersion

libraryDependencies += "com.typesafe.scala-logging" % "scala-logging-slf4j_2.10" % "2.1.2"

libraryDependencies += "com.holdenkarau" %% "spark-testing-base" % "1.6.0_0.8.0" % "test"

libraryDependencies += "org.scalatest" %% "scalatest" % "3.0.4" % "test"

assemblyMergeStrategy in assembly := {
  case PathList("javax", "servlet", xs @ _*) => MergeStrategy.last
  case PathList("javax", "activation", xs @ _*) => MergeStrategy.last
  case PathList("org", "apache", xs @ _*) => MergeStrategy.last
  case PathList("com", "google", xs @ _*) => MergeStrategy.last
  case PathList("com", "esotericsoftware", xs @ _*) => MergeStrategy.last
  case PathList("com", "codahale", xs @ _*) => MergeStrategy.last
  case PathList("com", "yammer", xs @ _*) => MergeStrategy.last
  case PathList("commons-beanutils", "commons-beanutils", xs @ _*) => MergeStrategy.last
  case p if p.contains("parquet") => MergeStrategy.last
  case p if p.contains("overview.html") => MergeStrategy.last
  case p if p.contains("plugin.xml") => MergeStrategy.last
  case "about.html" => MergeStrategy.rename
  case "META-INF/ECLIPSEF.RSA" => MergeStrategy.last
  case "META-INF/mailcap" => MergeStrategy.last
  case "META-INF/mimetypes.default" => MergeStrategy.last
  case "plugin.properties" => MergeStrategy.last
  case "log4j.properties" => MergeStrategy.last
  case x =>
    val oldStrategy = (assemblyMergeStrategy in assembly).value
    oldStrategy(x)
}
    