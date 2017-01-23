package com.reevoo.snowplow.actions

import com.github.nscala_time.time.Imports._
import com.reevoo.snowplow.metrics.{NumberOfClickConvertedDidntClickConvertedPerTrkrefPerDay,
                                    NumberOfRenderedBadgesAndDistinctUserClicksAndNonClicksPerTrkrefPerDay,
                                    TotalSessionTimeWithClickAndWithNotClickPerTrkrefPerDay}

object CalculateOverviewDashboardAggregates {

  def execute(dateRange: (DateTime, DateTime)): Unit = {
    // TODO -->  reset to zero the dates in the range before recalculating????
    NumberOfRenderedBadgesAndDistinctUserClicksAndNonClicksPerTrkrefPerDay.execute(dateRange)
    TotalSessionTimeWithClickAndWithNotClickPerTrkrefPerDay.execute(dateRange)
    NumberOfClickConvertedDidntClickConvertedPerTrkrefPerDay.execute(dateRange)
  }

}
