package com.reevoo.snowplow

import com.reevoo.snowplow.TimeUtils.time
import com.reevoo.snowplow.actions._
import com.github.nscala_time.time.Imports.DateTime
import org.joda.time.Days
import com.frugalmechanic.optparse._

/**
  * Explain what this is meant to do, how often needs to be run (only once), etc..*[]:
  *
  */
object HistoricalS3DumpsAggregatesGenerator {


  object CommandLineArguments extends OptParse {
    val fromDate = StrOpt()
    val toDate = StrOpt()
    val overwriteExistingAggregates = BoolOpt()
  }

  def main(args: Array[String]) {

    CommandLineArguments.parse(args)

    time(s"Running HistoricalS3DumpsAggregatesGenerator with arguments: ${args.mkString(" ")}") {
      val fromDate = new DateTime(CommandLineArguments.fromDate.get)
      val toDate = new DateTime(CommandLineArguments.toDate.get)

      (0 to Days.daysBetween(fromDate, toDate).getDays()).map(fromDate.plusDays(_)).foreach(date => {
        time(s"processing S3 folder dated $date...") {
          try {
            val dateRangeLoaded = UploadMarkEventsFromS3.execute(date)
            MarkEventsETL.execute()
            if (CommandLineArguments.overwriteExistingAggregates) {
              DeleteAggregates.execute(dateRangeLoaded)
            }
            CalculateOverviewDashboardAggregates.execute(dateRangeLoaded)
            UnloadMarkEventsToS3.execute(Database.MarkEventsStagingTableName, dateRangeLoaded._2)
          } catch {
            case e: Throwable => println(s"Error while processing s3 folder dated $date with exception $e")
          }
        }
      })
    }
  }

}


