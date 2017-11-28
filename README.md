Spark Movie Ratings
===================

- Read in movies.dat and ratings.dat to spark dataframes called movies and ratings respectively
- Create a new dataframe called movieRatings, containing the movies data and 3 new columns which contain the max, min and average rating for that movie from the ratings data.
- Create a new dataframe which contains each user’s (userId in the ratings data) top 3 movies based on their rating.
- Write out the original and new dataframes to parquet format.


`spark-submit \
  --class com.newday.Driver \
  --master yarn \ 
  /my-jar-location/SparkMovieRatings-assembly-1.0.jar \
    movies-input-path.dat \
    ratings-input-path.dat \
    output-path`