package com.reevoo.snowplow.redshift.queries.metrics

import com.github.nscala_time.time.Imports._
import com.reevoo.snowplow.RedshiftService

object TotalSessionTimeWithClickPerTrkrefPerDay {

  def execute(dateRange: Tuple2[DateTime, DateTime]) = {
    val snowplowDatabase = RedshiftService.snowplowDatabase


    val connection = snowplowDatabase.getConnection
    try {

      val resultSet = snowplowDatabase.executeQuery(this.query(dateRange), connection)

      val tableauDatabase = RedshiftService.tableauDatabase
      val tableauConnection = tableauDatabase.getConnection

      while (resultSet.next) {
        insertAggregatesIntoTableauDb(resultSet, tableauDatabase, tableauConnection)
      }

    } finally {
      connection.close
    }
  }


  private def query(dateRange: Tuple2[DateTime, DateTime]) = {
    s"""
       |select date, date_week, date_month, trkref, sum(session_duration) as session_duration_sum
       |from (
       |	select to_date(DATE_TRUNC('day', derived_tstamp), 'YYYY-MM-DD') as date,
       |       to_date(date_trunc('week', derived_tstamp::timestamp), 'YYYY-MM-DD') as date_week,
       |       to_date(date_trunc('month', derived_tstamp::timestamp), 'YYYY-MM-DD') as date_month,
       |       trkref,
       |       domain_sessionid,
       |       datediff(seconds, min(derived_tstamp), max(derived_tstamp)) as session_duration,
       |       sum((CASE event_type WHEN 'clicked' THEN 1 ELSE 0 END)) as clicks_count
       |	from atomic.mark_events
       |	where trkref is not null
       |	and derived_tstamp between '${dateRange._1}' and '${dateRange._2}'
       |	group by 1, 2, 3, 4, 5
       |)
       |where session_duration > 0
       |and clicks_count > 0
       |group by 1, 2, 3, 4
      """.stripMargin
  }

  private def insertAggregatesIntoTableauDb(resultSet: java.sql.ResultSet, database: RedshiftService, connection: java.sql.Connection) = {

    val query =       s"""
                         | UPDATE overview_dashboard_data_testing set clicked_time_on_site=${resultSet.getLong("session_duration_sum")}
                         | WHERE trkref='${resultSet.getString("trkref")}'
                         | AND date_day='${resultSet.getDate("date")}'
                        """.stripMargin

    println(query)
    database.executeUpdate(query, connection)

  }
}
