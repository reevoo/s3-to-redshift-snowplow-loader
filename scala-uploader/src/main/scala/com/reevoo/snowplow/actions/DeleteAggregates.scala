package com.reevoo.snowplow.actions

import com.github.nscala_time.time.Imports._
import com.reevoo.snowplow.Database
import com.reevoo.snowplow.TimeUtils._

object DeleteAggregates {

  def execute(dateRange: (DateTime, DateTime)) = {
    UpdateDBQuery.execute(
      Database.Tableau,
      s"""DELETE FROM ${Database.OverviewDashboardDataTableName}
          | WHERE date_day between  ${DateFormatter.print(dateRange._1)}
          | and ${DateFormatter.print(dateRange._2)}""".stripMargin
    )
  }
}
