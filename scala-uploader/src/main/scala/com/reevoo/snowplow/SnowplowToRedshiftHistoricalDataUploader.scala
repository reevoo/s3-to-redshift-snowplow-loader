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

    time("Uploading started...") {
      if (CommandLineArguments.fromDate && CommandLineArguments.toDate) {
        val DateFormatter = DateTimeFormat.forPattern("yyyy-MM-dd")
        val fromDate = new DateTime(CommandLineArguments.fromDate.get)
        val toDate = new DateTime(CommandLineArguments.toDate.get)

        redshiftService.createOrTruncateAggregatedEventsTable(CommandLineArguments.overwrite)

        (0 to Days.daysBetween(fromDate, toDate).getDays()).map(fromDate.plusDays(_)).foreach(date => {
          time(s"*** PROCESSING FILES FOR DATE ${date} FOLDERS") {
            redshiftService.uploadEvents(DateFormatter.print(date))
          }
        })

      }

      if (CommandLineArguments.initial) {
        time(s"*** PROCESSING FILES FOR _INITIAL FOLDER") {
          redshiftService.uploadEvents("_initial")
        }
      }
    }

  }


}
