package com.reevoo.snowplow.actions

import com.reevoo.snowplow.TimeUtils.time
import com.reevoo.snowplow.metrics.{NumberOfClickConvertedDidntClickConvertedPerTrkrefPerDay,
                                    NumberOfRenderedBadgesAndDistinctUserClicksAndNonClicksPerTrkrefPerDay,
                                    TotalSessionTimeWithClickAndWithNotClickPerTrkrefPerDay}
import com.github.nscala_time.time.Imports.DateTime

object CalculateOverviewDashboardAggregates {

  def execute(dateRange: (DateTime, DateTime)): Unit = {
    time(s"Calculating Aggregates for date range $dateRange") {
      println("Calculating NumberOfRenderedBadgesAndDistinctUserClicksAndNonClicksPerTrkrefPerDay")
      NumberOfRenderedBadgesAndDistinctUserClicksAndNonClicksPerTrkrefPerDay.executeByRange(dateRange)
      println("Calculating TotalSessionTimeWithClickAndWithNotClickPerTrkrefPerDay")
      TotalSessionTimeWithClickAndWithNotClickPerTrkrefPerDay.executeByRange(dateRange)
      println("Calculating NumberOfClickConvertedDidntClickConvertedPerTrkrefPerDay")
      NumberOfClickConvertedDidntClickConvertedPerTrkrefPerDay.executeByDay(dateRange)
    }
  }

}
