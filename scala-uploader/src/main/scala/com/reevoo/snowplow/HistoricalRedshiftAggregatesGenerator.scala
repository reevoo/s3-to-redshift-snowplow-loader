package com.reevoo.snowplow

import com.reevoo.snowplow.actions.{GetMinAndMaxDateIntervalFromDBTable, CalculateOverviewDashboardAggregates, UnloadMarkEventsToS3}
import com.github.nscala_time.time.Imports.DateTime

/**
  * This will run directly in the atomic.mark_events table (only once), to go through the historic data that we
  * have there for all the trkrefs that we currently are always keeping in the table and not offloading to S3.
  */
object HistoricalRedshiftAggregatesGenerator {

  def main(args: Array[String]) {
    val dateRangeToProcess = GetMinAndMaxDateIntervalFromDBTable.execute(Database.Snowplow, "atomic.mark_events", "collector_tstamp")
    CalculateOverviewDashboardAggregates.execute(dateRangeToProcess)
    UnloadMarkEventsToS3.execute(Database.MarkEventsStagingTableName, dateRangeToProcess._2)
  }

}
