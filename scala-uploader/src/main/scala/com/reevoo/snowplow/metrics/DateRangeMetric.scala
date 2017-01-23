package com.reevoo.snowplow.metrics

import com.github.nscala_time.time.Imports.{DateTime, DateTimeFormat}
import org.joda.time.Days
import com.reevoo.snowplow.Database
import java.sql.{Connection, ResultSet, Statement}

import com.reevoo.snowplow.actions.GetTotalRowCountFromDBTable

trait DateRangeMetric {

  final val DateFormatter = DateTimeFormat.forPattern("yyyy-MM-dd")
  final val AggregatesTableName = "overview_dashboard_data_testing"

  /**
    *
    * @param dateRange
    */
  def execute(dateRange: (DateTime, DateTime)) = {
    var snowplowConnection: Connection = null
    var snowplowSatement: Statement = null

    try {
      snowplowConnection = Database.Snowplow.getConnection
      snowplowSatement = snowplowConnection.createStatement()

      (0 to Days.daysBetween(dateRange._1, dateRange._2).getDays()).map(dateRange._1.plusDays(_)).foreach(date => {
        println(s"Processing day $date")
        val metricRowsToInsert = snowplowSatement.executeQuery(metricSelectionSQLQuery(date))
        while (metricRowsToInsert.next) {
          insertAggregateIntoTableauDb(metricRowsToInsert)
        }
      })

    } finally {
      if (snowplowSatement != null) snowplowSatement.close()
      if (snowplowConnection != null) snowplowConnection.close()
    }
  }

  /**
    *
    * @param metricRow
    */
  def insertAggregateIntoTableauDb(metricRow: ResultSet): Unit = {

    var tableauConnection: Connection = null
    var statement:Statement = null
    try {

      tableauConnection = Database.Tableau.getConnection
      statement = tableauConnection.createStatement()

      val aggregateFowExists = GetTotalRowCountFromDBTable.execute(
        tableauConnection,
        "overview_dashboard_data_testing",
        Map("trkref" -> metricRow.getString("trkref"), "date_day" -> metricRow.getString("date"))) > 0

      if (aggregateFowExists) {
        statement.executeUpdate(aggregatePerTrkrefPerDayUpdateQuery(metricRow))
      } else {
        statement.executeUpdate(aggregatePerTrkrefPerDayCreationQuery(metricRow))
        statement.executeUpdate(aggregatePerTrkrefPerDayUpdateQuery(metricRow))
      }
    } finally {
      if (statement != null) statement.close()
      if (tableauConnection != null) tableauConnection.close()
    }

  }

  /**
    *
    * @param metricsRow
    * @return
    */
  def aggregatePerTrkrefPerDayCreationQuery(metricsRow: ResultSet): String = {
    s"""
       | INSERT INTO $AggregatesTableName ("trkref", "date_day", "date_week", "date_month")
       | VALUES (
       | '${metricsRow.getString("trkref")}',
       | '${metricsRow.getDate("date")}',
       | '${metricsRow.getDate("date_week")}',
       | '${metricsRow.getDate("date_month")}'
       | )""".stripMargin
  }

  /**
    *
    * @param metricsRow
    * @return
    */
  def aggregatePerTrkrefPerDayUpdateQuery(metricsRow: ResultSet): String


  /**
    *
    * @param date
    * @return
    */
  def metricSelectionSQLQuery(date: DateTime): String

}
