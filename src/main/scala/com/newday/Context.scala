package com.newday

import java.util.Properties

import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.{SparkConf, SparkContext}

import scala.io.Source

object Context {
  var conf: SparkConf = _
  var sc: SparkContext = _
  var sqlContext: Option[HiveContext] = _

  def init(sqlContext: Option[HiveContext]=None): Unit = {
    sqlContext match {
      case None => {

        def initializeRealExecution: Unit = {
          val properties = new Properties

          val url = getClass.getResource("/App.properties")
          if (url != null) {
            val source = Source.fromURL(url)
            properties.load(source.bufferedReader())
          }

          val conf = new SparkConf()
            .setAppName(properties.getProperty("appName", "Movie Ratings"))
            .setMaster(properties.getProperty("master", "local[*]"))
          val sc = new SparkContext(conf)
          this.sqlContext = Some(new HiveContext(sc))
        }

        initializeRealExecution
      }
      case Some(s) => this.sqlContext = sqlContext
    }
  }

  def stop(): Unit = {
    sc.stop()
  }

}
