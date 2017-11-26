package com.newday.utils

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

object Reader {
  def reader(path :String,delimiter: String="::")(implicit sc:SparkContext): RDD[Array[String]] ={
    sc.textFile(path).map(value => value.split(delimiter))
  }
}
