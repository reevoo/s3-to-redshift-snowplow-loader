package com.reevoo.snowplow.redshift.queries.metrics

import com.github.nscala_time.time.Imports._
import com.reevoo.snowplow.RedshiftService
import com.reevoo.snowplow.redshift.queries.TotalRowCount

object NumberOfRenderedBadgesAndDistinctUserClicksAndNonClicksPerTrkrefPerDay {

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
       |SELECT date,
       |       date_week,
       |       date_month,
       |       trkref,
       |       SUM(CASE WHEN event_type = 'clicked' THEN number_of_events ELSE 0 END) AS clicked,
       |       SUM(CASE WHEN event_type = 'rendered' THEN number_of_events ELSE 0 END) AS rendered,
       |       (SUM(CASE WHEN event_type = 'rendered' THEN number_of_events ELSE 0 END) - SUM(CASE WHEN event_type = 'clicked' THEN number_of_events ELSE 0 END)) as didnt_click
       |FROM (
       |	SELECT to_date(DATE_TRUNC('day', derived_tstamp), 'YYYY-MM-DD') AS date,
       |       to_date(date_trunc('week', derived_tstamp::timestamp), 'YYYY-MM-DD') AS date_week,
       |       to_date(date_trunc('month', derived_tstamp::timestamp), 'YYYY-MM-DD') AS date_month,
       |       trkref,
       |       event_type,
       |       COUNT(DISTINCT(domain_userid)) AS number_of_events
       |FROM atomic.mark_events
       |WHERE trkref IS NOT NULL
       |AND derived_tstamp BETWEEN '${dateRange._1}' and '${dateRange._2}'
       |AND event_type in ('rendered','clicked')
       |GROUP BY 1, 2, 3, 4, 5
       |) GROUP BY 1,2,3,4
       |
      """.stripMargin
  }

  private def insertAggregatesIntoTableauDb(resultSet: java.sql.ResultSet, database: RedshiftService, connection: java.sql.Connection) = {

    val insertQuery =
      s"""
         | INSERT INTO overview_dashboard_data_testing ("trkref", "date_day", "date_week", "date_month")
         | VALUES (
         | '${resultSet.getString("trkref")}',
         | '${resultSet.getDate("date")}',
         | '${resultSet.getDate("date_week")}',
         | '${resultSet.getDate("date_month")}'
         | )""".stripMargin

    val updateQuery =
      s"""
         | UPDATE overview_dashboard_data_testing
         | SET clicked=nvl(clicked, 0) + ${resultSet.getInt("clicked")},
         |     didnt_click=nvl(didnt_click, 0) + ${resultSet.getInt("didnt_click")},
         |     rendered=nvl(rendered, 0) + ${resultSet.getInt("rendered")}
         | WHERE trkref='${resultSet.getString("trkref")}'
         | AND date_day='${resultSet.getDate("date")}'""".stripMargin


    val rowExists = TotalRowCount.execute(
      database,
      connection,
      "overview_dashboard_data_testing",
      Map("trkref" -> resultSet.getString("trkref"), "date_day" -> resultSet.getString("date"))) > 0


    if (rowExists) {
      println(updateQuery)
      database.executeUpdate(updateQuery, connection)
    } else {
      println(insertQuery)
      database.executeUpdate(insertQuery, connection)
      database.executeUpdate(updateQuery, connection)
    }

  }

}
