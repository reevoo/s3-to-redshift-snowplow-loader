package com.reevoo.snowplow

import com.reevoo.snowplow.TimeUtils._
import com.github.nscala_time.time.Imports._
import org.joda.time.Days
import com.frugalmechanic.optparse._

import com.reevoo.snowplow.redshift.queries._
import com.reevoo.snowplow.redshift.queries.metrics._

object SnowplowToRedshiftHistoricalDataUploader {

  object CommandLineArguments extends OptParse {
    val initial = BoolOpt()
    val fromDate = StrOpt()
    val toDate = StrOpt()
    val overwrite = BoolOpt()
  }

  final val EventsFolderToTableName = Map(
    "events" -> "atomic.root_events_upload_from_s3",
    "com_reevoo_badge_event_1" -> "atomic.badge_events_uploaded_from_s3",
    "com_reevoo_conversion_event_1" -> "atomic.conversion_events_uploaded_from_s3"
  )

  final val DateFormatter = DateTimeFormat.forPattern("yyyy-MM-dd")

  def main(args: Array[String]) {

    CommandLineArguments.parse(args)

    time("Uploading started...") {
      if (CommandLineArguments.fromDate && CommandLineArguments.toDate) {
        val fromDate = new DateTime(CommandLineArguments.fromDate.get)
        val toDate = new DateTime(CommandLineArguments.toDate.get)

        (0 to Days.daysBetween(fromDate, toDate).getDays()).map(fromDate.plusDays(_)).foreach(date => {
          time(s"*** PROCESSING FILES FOR DATE ${date} FOLDERS") {
            copyFromS3(date)

            val dateRangeMoved = moveToMarkEvents

            calculateAggregates(dateRangeMoved)

            unloadMarkEventsToS3(dateRangeMoved)
          }
        })
      }

    }
  }


  def copyFromS3(date: DateTime) = {
    time("copying events from S3 ...") {
      val s3Service = new AmazonS3Service()
      EventsFolderToTableName.keys.foreach(folderName => {
        val s3Urls = s3Service.getListOfFolders(folderName, DateFormatter.print(date))
        s3Urls.foreach {
          RedshiftService.snowplowDatabase.uploadToTable(EventsFolderToTableName(folderName), _)
        }
      })
    }
  }

  def moveToMarkEvents():Tuple2[DateTime, DateTime] = {
    val dateRangeToMove = MaxMinDateIntervalQuery.execute(
      tableName = "atomic.mark_events_from_s3",
      dateColumn = "collector_tstamp"
    )
    time("Moving events to mark_events table ...") {
      MarkEventsETLQuery.execute(dateRangeToMove)
      TruncateTableQuery.execute(EventsFolderToTableName.values)
    }
    (new DateTime(dateRangeToMove._1), new DateTime(dateRangeToMove._2))
  }


  def unloadMarkEventsToS3(dateRange: Tuple2[DateTime, DateTime]) = {
    (0 to Days.daysBetween(dateRange._1, dateRange._2).getDays()).map(dateRange._1.plusDays(_)).foreach(date => {
      MarkEventsUnloadQuery.execute("atomic.mark_events_from_s3", DateFormatter.print(date))
    })
  }

  def calculateAggregates(dateRange: Tuple2[DateTime, DateTime]) = {
//      NumberOfRenderedBadgesPerTrkrefPerDay.execute(dateRange)
//    NumberOfDistinctUserClicksPerTrkrefPerDay.execute(dateRange)
//    NumberOfDistinctUserNonClicksPerTrkrefPerDay.execute(dateRange)
//      TotalSessionTimeWithClickPerTrkrefPerDay.execute(dateRange)
//    TotalSessionTimeWithNoClickPerTrkrefPerDay.execute(dateRange)
    NumberOfClickConvertedDidntClickConvertedPerTrkrefPerDay.execute(dateRange)
  }

}


