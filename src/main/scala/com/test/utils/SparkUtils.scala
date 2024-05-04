package com.test.utils

import org.apache.log4j.{FileAppender, Logger}
import org.apache.spark.sql.SparkSession

object SparkUtils {

  def getSparkSession(logger: Logger, master: String, appName: String = "Spark Scala"): SparkSession = {
    logger.info("creating spark session object")
    var spark: SparkSession = null
    try{
      spark = SparkSession.builder().appName(appName).master(master).getOrCreate()
    } catch {
      case e: Exception => {
        logger.error("exception occured while creating spark session")
        logger.error(e.getMessage)
      }
    }
    spark
  }

}
