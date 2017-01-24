package com.reevoo.snowplow

import com.reevoo.snowplow.actions.{CalculateOverviewDashboardAggregates, GetMinAndMaxDateIntervalFromDBTable, UnloadMarkEventsToS3}
import com.github.nscala_time.time.Imports.DateTime


/**
  * Created by jesuslara on 1/24/17.
  */
object DailyAutomatedAggregatesGenerator {

  def main(args: Array[String]) {
    val startDate = new DateTime("2016-01-01") // "last day we have aggregates in tableau plus one day."
    val endDate = new DateTime("2016-01-01") // "todays date minus one day, to make sure we get complete days as today's date will still be getting new events"
    CalculateOverviewDashboardAggregates.execute((startDate, endDate))

    // make sure we only keep the last 60 days worth of events in the table by unloading anything that is older
    // than 60 days.
    val currentMarkEventsDateRange = GetMinAndMaxDateIntervalFromDBTable.execute(Database.Snowplow, "atomic.mark_events", "collector_tstamp")
    UnloadMarkEventsToS3.execute("atomic.mark_events", currentMarkEventsDateRange._2)
  }

}
