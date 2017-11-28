package com.newday

import com.newday.utils.Operations._
import com.newday.utils.Reader
import com.newday.utils.Writer._

object Job {
  def run(inputMoviesPath: String, inputRatingsPath: String, OutputPath: String) {

    val movies = Reader.readMovies(inputMoviesPath)
    val ratings = Reader.readRatings(inputRatingsPath)
    val movieRatings = minMaxAndAverageRatingForEachMovie(movies, ratings)
    val topThree = topThreeMoviesPerUser(ratings, movies)

    write(movies, s"$OutputPath/movies")
    write(ratings, s"$OutputPath/ratings")
    write(movieRatings, s"$OutputPath/movieRatings")
    write(topThree, s"$OutputPath/topThree")

  }
}
