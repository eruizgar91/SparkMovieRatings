package com.newday

import com.typesafe.scalalogging.slf4j.LazyLogging

import scala.util.{Failure, Success, Try}

object Driver extends LazyLogging{
  def main(args: Array[String]): Unit = {

    require(args.length == 3,
      "You should provide the movies.dat location,the ratings.dat location and the output path")
    Context.init()
    Try{Job.run(args(0), args(1), args(2))} match {
      case Success(_) => logger.info("Success starting the job")
      case Failure(ex) => logger.error(s"The job failed starting: [${ex.getMessage}]")
    }

  }
}
