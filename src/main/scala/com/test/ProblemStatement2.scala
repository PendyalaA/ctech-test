package com.test

import com.test.utils.{LogUtils, SparkUtils}
import org.apache.log4j.{FileAppender, Level, Logger}
import org.apache.spark.sql.functions.{col, countDistinct, substring}

object ProblemStatement2 {

  final val LOG_PATTERN: String = "%d %-5p %C%M:%L - %m%n"

  val appender: FileAppender = LogUtils.getFileAppender("ProblemStatement2", "src/main/resources/ProblemStatement2.log", LOG_PATTERN)

  val logger: Logger = Logger.getLogger(this.getClass)

  logger.setLevel(Level.DEBUG)
  logger.addAppender(appender)

  def main(args: Array[String]): Unit = {
    try{
      logger.info("job execution started!")

      val memberEligibilityFilePath = "src/main/resources/member_eligibility.csv"

      val memberMonthsFilePath = "src/main/resources/member_months.csv"

      val outputPath = "src/main/resources/total_number_of_member_months_per_member_per_year"

      val spark = SparkUtils.getSparkSession(logger, "local[*]", "test1")

      val memberEligibilityDF = spark.read.option("header", true).csv(memberEligibilityFilePath)
      val memberMonthsDF = spark.read.option("header", true).csv(memberMonthsFilePath)

      val memberDF = memberEligibilityDF.join(memberMonthsDF, Seq("member_id")).select(
        memberEligibilityDF("member_id")
        , memberMonthsDF("eligibility_member_month")
      )

      val memberAggregatedDF = memberDF.groupBy(col("member_id"), substring(col("eligibility_member_month"), 0, 4).as("year")).agg(countDistinct(col("eligibility_member_month")).as("number_of_member_months"))

      memberAggregatedDF.write.json(outputPath)

      logger.info("job completed!")
    }
    catch{
      case e: Exception => {
        logger.error(e.getMessage)
      }
    }
  }

}


