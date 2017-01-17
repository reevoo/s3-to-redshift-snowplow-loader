package com.reevoo.snowplow

import com.reevoo.snowplow.TimeUtils._
import com.github.nscala_time.time.Imports._
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

    val redshiftService = new RedshiftService("atomic.mark_events_historic")
    val s3Service = new AmazonS3Service()

    time("Uploading started...") {
      if (CommandLineArguments.fromDate && CommandLineArguments.toDate) {
        val DateFormatter = DateTimeFormat.forPattern("yyyy-MM-dd")
        val fromDate = new DateTime(CommandLineArguments.fromDate.get)
        val toDate = new DateTime(CommandLineArguments.toDate.get)

        val eventsFolderToTableName = Map(
          "events" -> "atomic.root_events_upload_from_s3",
          "com_reevoo_badge_event_1" -> "atomic.badge_events_uploaded_from_s3",
          "com_reevoo_conversion_event_1" -> "atomic.conversion_events_uploaded_from_s3"
        )

        (0 to Days.daysBetween(fromDate, toDate).getDays()).map(fromDate.plusDays(_)).foreach(date => {
          time(s"*** PROCESSING FILES FOR DATE ${date} FOLDERS") {

            eventsFolderToTableName.keys.foreach( folderName =>  {
              val s3Urls = s3Service.getListOfFolders(folderName , DateFormatter.print(date))
              s3Urls.foreach {
                redshiftService.uploadToTable(eventsFolderToTableName(folderName), _)
              }
            })

            // etl of the events into mark_events table by joining the temporary tables and empty the temporary tables.



            // calculate the aggregates and save the aggregates to the tableau redshift database.
            // unload the loaded data from mark_events back to a new s3 folder splitting the events by date subfolders



//            redshiftService.uploadEvents(DateFormatter.print(date))
          }
        })

      }

//      if (CommandLineArguments.initial) {
//        time(s"*** PROCESSING FILES FOR _INITIAL FOLDER") {
//          redshiftService.uploadEvents("_initial")
//        }
//      }
    }

  }


}
