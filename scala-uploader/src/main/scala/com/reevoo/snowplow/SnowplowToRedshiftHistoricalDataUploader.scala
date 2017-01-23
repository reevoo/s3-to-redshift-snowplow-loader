package com.reevoo.snowplow

import com.reevoo.snowplow.TimeUtils.time
import com.reevoo.snowplow.actions.{MarkEventsETL, UnloadMarkEventsToS3, CalculateOverviewDashboardAggregates, UploadMarkEventsFromS3}
import com.github.nscala_time.time.Imports.DateTime
import org.joda.time.Days
import com.frugalmechanic.optparse._

object SnowplowToRedshiftHistoricalDataUploader {

  object CommandLineArguments extends OptParse {
    val initial = BoolOpt()
    val fromDate = StrOpt()
    val toDate = StrOpt()
    val overwrite = BoolOpt()
  }

  def main(args: Array[String]) {

    CommandLineArguments.parse(args)

    time("Uploading started...") {
      if (CommandLineArguments.fromDate && CommandLineArguments.toDate) {
        val fromDate = new DateTime(CommandLineArguments.fromDate.get)
        val toDate = new DateTime(CommandLineArguments.toDate.get)

        (0 to Days.daysBetween(fromDate, toDate).getDays()).map(fromDate.plusDays(_)).foreach(date => {
          time(s"processing S3 folder dated $date...") {
            val dateRangeLoaded = UploadMarkEventsFromS3.execute(date)
            MarkEventsETL.execute(dateRangeLoaded)
            CalculateOverviewDashboardAggregates.execute(dateRangeLoaded)
            UnloadMarkEventsToS3.execute(Database.MarkEventsTableName, dateRangeLoaded)
          }
        })
      }
    }
  }

}


