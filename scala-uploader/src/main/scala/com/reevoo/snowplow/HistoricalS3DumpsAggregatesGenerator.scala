package com.reevoo.snowplow

import com.reevoo.snowplow.TimeUtils._
import com.reevoo.snowplow.actions._
import com.github.nscala_time.time.Imports.DateTime
import com.frugalmechanic.optparse._
import com.typesafe.scalalogging.LazyLogging


/**
  * Runner used to process all of the historic snowplow mark events data stored in S3, calculate aggregate values
  * based on them, and persist those aggregate values to a Tableau Redshift database.
  *
  * At the moment the historic mark events data in S3 are dumps of three different tables in the Snowplow Redshift
  * database. The ones below:
  *
  *   - landing.events
  *   - landing.com_reevoo_badge_event_1
  *   - landing.com_reevoo_conversion_event_1
  *
  * In order to calculate the aggregates we need to copy this data back to Snowplow, join them together into a single
  * mark_events table, and run SQL queries on them. But that is not possible as all of the S3 data wouldn't fit in the
  * database all at the same time.
  *
  * This runner is able to go through the whole stored data in chunks. It will upload a few of the files, join them
  * and calculate and persist the aggregates, and then remove the uploaded data from Redshift before moving on to the
  * next chunk of data. That way the current size of the Redshift database doesn't need to increase in order to process
  * all of the historic data.
  *
  * Also, as it goes through the data chunks, it will dump them in a new folder in S3 in a better format for future
  * processing. The new dump will be from a single table, instead of three separate ones, and also will be partitioned
  * by date folders in a consistent way, unlike the current dumps.
  *
  */
object HistoricalS3DumpsAggregatesGenerator extends LazyLogging {

  object CommandLineArguments extends OptParse {
    val fromDate = StrOpt()
    val toDate = StrOpt()
    val overwriteExistingAggregates = BoolOpt()
  }

  lazy val fromDate = new DateTime(CommandLineArguments.fromDate.get)
  lazy val toDate = new DateTime(CommandLineArguments.toDate.get)

  def main(args: Array[String]) {
    CommandLineArguments.parse(args)

    time(s"Running HistoricalS3DumpsAggregatesGenerator with arguments: ${args.mkString(" ")}") {
      if (CommandLineArguments.overwriteExistingAggregates) {
        deleteAggregatesForDateRange(fromDate, toDate)
      }
      listOfDaysBetween(fromDate, toDate).map(fromDate.plusDays).foreach(processAggregatesForDate)
    }
  }

  private def processAggregatesForDate(date: DateTime): Unit = {
    time(s"processing S3 folder dated ${DateFormatter.print(date)}...") {
      val dateRangeLoaded = UploadMarkEventsFromS3.execute(date)
      MarkEventsETL.execute()
      CalculateOverviewDashboardAggregates.execute(latest(dateRangeLoaded._1, fromDate), dateRangeLoaded._1)
      UnloadMarkEventsToS3.execute(Database.MarkEventsStagingTableName, dateRangeLoaded._2)
    }
  }

  private def deleteAggregatesForDateRange(dateRange: (DateTime, DateTime)) = {
    UpdateDBQuery.execute(
      Database.Tableau,
      s"""DELETE FROM ${Database.OverviewDashboardDataTableName}
          | WHERE date_day between  '${DateFormatter.print(dateRange._1)}'
          | and '${DateFormatter.print(dateRange._2)}'""".stripMargin
    )
  }

}


