package com.newday

import com.holdenkarau.spark.testing.SharedSparkContext
import com.newday.model.{Movies, Ratings}
import com.newday.utils.Operations
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.hive.HiveContext
import org.scalatest.FunSuite


class JobTest extends FunSuite with SharedSparkContext {

  case class MinMaxAvgDF(movieId: Int, title: String, genres: String, rating_min: Int, rating_max: Int, rating_avg: Double)

  case class TopThreeDF(userId: Int, favourite_movies: Seq[String])

  test("Gived a movie and 2 ratings with should obtain the dataframe containing movie and min,max and avg") {
    val input = Seq(Movies(1, "Star Wars: Episode two", "Fantasy|SciFi"))
    val input2 = Seq(Ratings(1, 1, 3, 1231234l), Ratings(2, 1, 5, 1235123523l))
    val sqlContext = SQLContext.getOrCreate(sc)
    val moviesDF = sqlContext.createDataFrame[Movies](input)
    val ratingsDF = sqlContext.createDataFrame[Ratings](input2)
    val expected = sqlContext.createDataFrame[MinMaxAvgDF](Seq(MinMaxAvgDF(1, "Star Wars: Episode two", "Fantasy|SciFi", 3, 5, 4.0)))
    assert(Operations.minMaxAndAverageRatingForEachMovie(moviesDF, ratingsDF).first === expected.first)
  }

  test("Given different rates for 2 users should calculate the top three of each user") {
    val input = Seq(Movies(1, "Movie 1", ""),
      Movies(2, "Movie 2", ""),
      Movies(3, "Movie 3", ""),
      Movies(4, "Movie 4", ""))
    val input2 = Seq(
      Ratings(1, 1, 2, 1l),
      Ratings(1, 2, 3, 2l),
      Ratings(1, 3, 4, 3l),
      Ratings(1, 4, 5, 4l),
      Ratings(2, 1, 5, 4l),
      Ratings(2, 2, 4, 3l),
      Ratings(2, 3, 3, 2l),
      Ratings(2, 4, 2, 1l)
    )
    val sqlContext = new HiveContext(sc)
    val moviesDF = sqlContext.createDataFrame[Movies](input)
    val ratingsDF = sqlContext.createDataFrame[Ratings](input2)
    val expected = sqlContext.createDataFrame[TopThreeDF](Seq(
      TopThreeDF(1, Seq("Movie 4", "Movie 3", "Movie 2")),
      TopThreeDF(2, Seq("Movie 1", "Movie 2", "Movie 3"))
    ))
    assert(Operations.topThreeMoviesPerUser(ratingsDF, moviesDF).take(2) === expected.take(2))

  }
}
