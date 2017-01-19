package com.reevoo.snowplow.redshift.queries.metrics


import com.reevoo.snowplow.RedshiftService
import com.github.nscala_time.time.Imports._

object NumberOfRenderedBadgesPerTrkrefPerDay {

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
       |       COUNT(DISTINCT(domain_userid)) as rendered
       |  from atomic.mark_events
       |  where trkref is not null
       |  and derived_tstamp between '${dateRange._1}' and '${dateRange._2}'
       |  and event_type='rendered'
       |  and hit_type='impression'
       |  group by 1, 2, 3, 4
      """.stripMargin
  }


  private def insertAggregatesIntoTableauDb(resultSet: java.sql.ResultSet, database: RedshiftService, connection: java.sql.Connection) = {

    val query =       s"""
                         | INSERT INTO overview_dashboard_data_testing ("trkref", "date_day", "date_week", "date_month", "rendered")
                         | VALUES (
                         | '${resultSet.getString("trkref")}',
                         | '${resultSet.getDate("date")}',
                         | '${resultSet.getDate("date_week")}',
                         | '${resultSet.getDate("date_month")}',
                         |  ${resultSet.getInt("rendered")}
                         | )
                        """.stripMargin

    println(query)
    database.executeUpdate(query, connection)

  }

}

