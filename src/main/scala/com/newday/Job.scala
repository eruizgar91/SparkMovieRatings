package com.newday

import java.util.Properties

import com.newday.model.{Movies, Ratings}
import com.newday.utils.Operations._
import com.newday.utils.Reader._
import com.newday.utils.Writer._
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.{SparkConf, SparkContext}

import scala.io.Source

object Job {
  def run(inputMoviesPath: String, inputRatingsPath: String, OutputPath: String) {
    val properties = new Properties()
    val url = getClass.getResource("/App.properties")
    if (url != null) {
      val source = Source.fromURL(url)
      properties.load(source.bufferedReader())
    }
    val conf = new SparkConf()
      .setAppName(properties.getProperty("appName", "Movie Ratings"))
      .setMaster(properties.getProperty("master", "local[*]"))
    val sc = new SparkContext(conf)
    val sqlContext = new HiveContext(sc)
    import sqlContext.implicits._

    val movies = reader(inputMoviesPath)(sc).map(m => Movies(m(0).toInt, m(1), m(2))).toDF().persist()
    val ratings = reader(inputRatingsPath)(sc)
      .map(r => Ratings(r(0).toInt, r(1).toInt, r(2).toInt, r(3).toLong)).toDF.persist()
    val movieRatings = minMaxAndAverageRatingForEachMovie(movies, ratings)
    val topThree = topThreeMoviesPerUser(ratings, movies)

    write(movies, s"$OutputPath/movies")
    write(ratings, s"$OutputPath/ratings")
    write(movieRatings, s"$OutputPath/movieRatings")
    write(topThree, s"$OutputPath/topThree")

    sc.stop()
  }
}
