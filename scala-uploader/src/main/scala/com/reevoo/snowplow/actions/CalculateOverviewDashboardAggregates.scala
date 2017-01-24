package com.reevoo.snowplow.actions

import com.reevoo.snowplow.TimeUtils.time
import com.reevoo.snowplow.metrics.{NumberOfClickConvertedDidntClickConvertedPerTrkrefPerDay,
                                    NumberOfRenderedBadgesAndDistinctUserClicksAndNonClicksPerTrkrefPerDay,
                                    TotalSessionTimeWithClickAndWithNotClickPerTrkrefPerDay}
import com.github.nscala_time.time.Imports.DateTime

object CalculateOverviewDashboardAggregates {

  def execute(dateRange: (DateTime, DateTime)): Unit = {
    time(s"Calculating Aggregates for date range $dateRange") {
      NumberOfRenderedBadgesAndDistinctUserClicksAndNonClicksPerTrkrefPerDay.execute(dateRange)
      TotalSessionTimeWithClickAndWithNotClickPerTrkrefPerDay.execute(dateRange)
      NumberOfClickConvertedDidntClickConvertedPerTrkrefPerDay.execute(dateRange)
    }
  }

}
