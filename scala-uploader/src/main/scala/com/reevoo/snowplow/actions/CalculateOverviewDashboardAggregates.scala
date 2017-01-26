package com.reevoo.snowplow.actions

import com.reevoo.snowplow.TimeUtils.time
import com.reevoo.snowplow.metrics._
import com.github.nscala_time.time.Imports.DateTime
import com.typesafe.scalalogging.LazyLogging

/**
  * Run all the aggregates needed in the relevant Snowplow Redshift tables and insert the calcualted values
  * in the relevant Tableau Redshift tables.
  */
object CalculateOverviewDashboardAggregates extends LazyLogging {

  /**
    * Trigers the calculation of the aggregates for the specified date range.
    *
    * @param dateRange Date range for which to calculate the aggregates.
    */
  def execute(dateRange: (DateTime, DateTime)): Unit = {
    time(s"Calculating Aggregates for date range $dateRange") {
      logger.info("Calculating NumberOfRenderedBadgesAndDistinctUserClicksAndNonClicksPerTrkrefPerDay")
      NumberOfRenderedBadgesAndDistinctUserClicksAndNonClicksPerTrkrefPerDay.executeByRange(dateRange)

      logger.info("Calculating TotalSessionTimeWithClickAndWithNotClickPerTrkrefPerDay")
      TotalSessionTimeWithClickAndWithNotClickPerTrkrefPerDay.executeByRange(dateRange)

      logger.info("Calculating NumberOfClickConvertedDidntClickConvertedPerTrkrefPerDay")
      NumberOfClickConvertedDidntClickConvertedPerTrkrefPerDay.executeByDay(dateRange)
    }
  }

}
