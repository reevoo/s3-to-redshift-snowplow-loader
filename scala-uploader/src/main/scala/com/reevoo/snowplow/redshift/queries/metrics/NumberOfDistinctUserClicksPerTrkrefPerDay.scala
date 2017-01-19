package com.reevoo.snowplow.redshift.queries.metrics

import com.github.nscala_time.time.Imports._
import com.reevoo.snowplow.RedshiftService

object NumberOfDistinctUserClicksPerTrkrefPerDay {

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
       |  select to_date(DATE_TRUNC('day', derived_tstamp), 'YYYY-MM-DD') as date,
       |       to_date(date_trunc('week', derived_tstamp::timestamp), 'YYYY-MM-DD') as date_week,
       |       to_date(date_trunc('month', derived_tstamp::timestamp), 'YYYY-MM-DD') as date_month,
       |       trkref,
       |       COUNT(DISTINCT(domain_userid)) as clicked
       |  from atomic.mark_events
       |  where trkref is not null
       |  and derived_tstamp between '${dateRange._1}' and '${dateRange._2}'
       |  and event_type='clicked'
       |  group by 1, 2, 3, 4
      """.stripMargin
  }

  private def insertAggregatesIntoTableauDb(resultSet: java.sql.ResultSet, database: RedshiftService, connection: java.sql.Connection) = {

    val query =       s"""
                         | UPDATE overview_dashboard_data_testing set clicked=${resultSet.getInt("clicked")}
                         | WHERE trkref='${resultSet.getString("trkref")}'
                         | AND date_day='${resultSet.getDate("date")}'
                        """.stripMargin

    println(query)
    database.executeUpdate(query, connection)

  }
}
