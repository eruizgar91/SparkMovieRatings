spark-submit \
  --class com.newday.Driver \
  --master yarn \
  --queue spark \
  --num-executors 60 \
  --executor-cores 4 \
  --executor-memory 5g \
  --deploy-mode client \
  /my-jar-location/SparkMovieRatings-assembly-1.0.jar \
    movies-input-path.dat \
    ratings-input-path.dat \
    output-path