package com.newday.utils

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.{avg, max, min}

object Operations {
  def minMaxAndAverageRatingForEachMovie(moviesDataFrame: DataFrame, ratingsDataFrame: DataFrame): DataFrame = {
    moviesDataFrame.join(
      ratingsDataFrame
        .groupBy("movieId")
        .agg(
          min("rating").as("rating_min"),
          max("rating").as("rating_max"),
          avg("rating").as("rating_avg")),
      Seq("movieId"),
      "left")
  }

  def topThreeMoviesPerUser(ratingsDataFrame: DataFrame,
                            moviesDataFrame: DataFrame): DataFrame = {

    import org.apache.spark.sql.expressions.Window
    import org.apache.spark.sql.functions.{collect_list, row_number}
    import ratingsDataFrame.sqlContext.implicits._


    // chose movie_id as secondary ordering column to keep the function deterministic
    val window = Window.partitionBy("userId").orderBy('rating.desc, 'movieId.desc)

    ratingsDataFrame
      .withColumn("rank", row_number.over(window))
      .filter('rank <= 3)
      .join(moviesDataFrame, Seq("movieId"))
      .groupBy("userId")
      .agg(collect_list("title").as("favourite_movies"))
      .select("userId", "favourite_movies")
  }

}
