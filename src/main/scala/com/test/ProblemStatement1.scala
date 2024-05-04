package com.test

import com.test.utils.{LogUtils, SparkUtils}
import org.apache.log4j.{FileAppender, Level, Logger}
import org.apache.spark.sql.functions.{concat, lit, count, col, first}

object ProblemStatement1 {

  val appender: FileAppender = LogUtils.getFileAppender("ProblemStatement1", "src/main/resources/ProblemStatement1.log")

  val logger: Logger = Logger.getLogger(this.getClass)

  logger.setLevel(Level.DEBUG)
  logger.addAppender(appender)

  def main(args: Array[String]): Unit = {

    try{
      logger.info("job execution started")
      val memberEligibilityFilePath = "src/main/resources/member_eligibility.csv"

      val memberMonthsFilePath = "src/main/resources/member_months.csv"

      val outputPath = "src/main/resources/total_number_of_member_months"

      val spark = SparkUtils.getSparkSession(logger, "local[*]", "test1")

      val memberEligibilityDF = spark.read.option("header", true).csv(memberEligibilityFilePath)
      val memberMonthsDF = spark.read.option("header", true).csv(memberMonthsFilePath)

      val memberDF = memberEligibilityDF.join(memberMonthsDF, Seq("member_id")).select(
        memberEligibilityDF("member_id")
        , concat(memberEligibilityDF("first_name"), lit(" "), memberEligibilityDF("middle_name"), lit(" "), memberEligibilityDF("last_name")).as("full_name")
        , memberMonthsDF("eligibility_member_month")
      )

      val memberAggregatedDF = memberDF.groupBy("member_id").agg(first(col("full_name")).as("full_name"), count(col("eligibility_member_month")).as("number_of_member_months"))

      memberAggregatedDF.write.partitionBy("member_id").json(outputPath)

      logger.info("output file created!")
    }
    catch{
      case e: Exception => {
        logger.error(e.getMessage)
        e.printStackTrace()
      }
    }
    finally {
      appender.close()
    }

  }

}


