import model.{Movies, Ratings}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.DataFrame

/**
  * Created by eruiz on 23/11/17.
  */
object Driver {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
      .setAppName("Simple Application")
      .setMaster("local[2]")
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)

    import sqlContext.implicits._

    val movies = sc.textFile("src/main/resources/ml-1m/movies.dat")
      .map(x => x.split("::"))
        .map(m=>Movies(m(0).toInt,m(1),m(2))).toDF().persist()

    val ratings = sc.textFile("src/main/resources/ml-1m/ratings.dat")
      .map(x => x.split("::"))
        .map(r=>Ratings(r(0).toInt,r(1).toInt,r(2).toInt,r(3).toLong)).toDF.persist()
    val movieRatings = extractMinMaxAverageRatingForEachMovie(movies,ratings)

    movies.coalesce(1).write.parquet("target/movies")
    ratings.coalesce(1).write.parquet("target/ratings")
    movieRatings.coalesce(1).write.parquet("target/movieRatings")

    println(movies.show)
    println(ratings.show)
    println(movieRatings.show)
    sc.stop()

  }



  def extractMinMaxAverageRatingForEachMovie(moviesDataFrame: DataFrame,ratingsDataFrame: DataFrame): DataFrame = {

    import org.apache.spark.sql.functions.{avg, broadcast, max, min}

    // after the agg operations, the ratings data will have less rows (assuming at least 1 movie does not have a rating)
    // and one more column (but the column values are on average much less data size than in the movies data), so
    // will broadcast the aggregated ratings, as we assume that would be smaller
    moviesDataFrame.join(broadcast(
      ratingsDataFrame
        .groupBy("movieId")
        .agg(
          min("rating").as("rating_min"),
          max("rating").as("rating_max"),
          avg("rating").as("rating_avg"))),
      Seq("movieId"),
      "left")
  }
}
