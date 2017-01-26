package com.reevoo.snowplow

import com.reevoo.snowplow.actions.{GetMinAndMaxDateIntervalFromDBTable, CalculateOverviewDashboardAggregates, UnloadMarkEventsToS3}
import com.github.nscala_time.time.Imports.DateTime

/**
  * WORK IN PROGRESS - JUST PLACEHOLDER
  */
object HistoricalRedshiftAggregatesGenerator {

  def main(args: Array[String]) {
    val dateRangeToProcess = GetMinAndMaxDateIntervalFromDBTable.execute(Database.Snowplow, Database.MarkEventsStagingTableName, "collector_tstamp")
    CalculateOverviewDashboardAggregates.execute(dateRangeToProcess)
    UnloadMarkEventsToS3.execute(Database.MarkEventsStagingTableName, dateRangeToProcess._2)
  }

}
