package com.newday.utils

import com.newday.Context
import com.newday.model.{Movies, Ratings, Users}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.hive.HiveContext

object Reader {
  val sqlContext: HiveContext = Context.sqlContext.get
  import sqlContext.implicits._

  def reader(path :String,delimiter: String="::"): RDD[Array[String]] ={
    sqlContext.sparkContext.textFile(path).map(value => value.split(delimiter))
  }
  def readMovies(path: String): DataFrame ={
    reader(path).map(m => Movies(m(0).toInt, m(1), m(2))).toDF().persist()
  }
  def readRatings(path: String): DataFrame = {
    reader(path).map(r => Ratings(r(0).toInt, r(1).toInt, r(2).toInt, r(3).toLong)).toDF.persist()
  }
  def readUsers(path: String): DataFrame ={
    reader(path).map(m => Users(m(0).toInt, m(1), m(2).toInt,m(3).toInt,m(4).toInt)).toDF().persist()
  }

}
