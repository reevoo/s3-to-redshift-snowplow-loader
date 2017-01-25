package com.reevoo.snowplow.metrics

import com.github.nscala_time.time.Imports.{DateTime, DateTimeFormat}
import org.joda.time.Days
import com.reevoo.snowplow.Database
import java.sql.{Connection, ResultSet, Statement}

trait DateRangeMetric {

  final val DateFormatter = DateTimeFormat.forPattern("yyyy-MM-dd")
  final val AggregatesTableName = "overview_dashboard_data_testing"

  /**
    *
    * @param dateRange
    */
  def executeByDay(dateRange: (DateTime, DateTime)) = {
    var snowplowConnection: Connection = null
    var snowplowSatement: Statement = null

    try {
      snowplowConnection = Database.Snowplow.getConnection
      snowplowSatement = snowplowConnection.createStatement()

      (0 to Days.daysBetween(dateRange._1, dateRange._2).getDays()).map(dateRange._1.plusDays(_)).foreach(date => {
        val metricRowsToInsert = snowplowSatement.executeQuery(metricSelectionSQLQuery(date, date.withTime(23,59,59,999)))
        while (metricRowsToInsert.next) {
          insertAggregateIntoTableauDb(metricRowsToInsert)
        }
      })

    } finally {
      if (snowplowSatement != null && !snowplowConnection.isClosed) snowplowSatement.close()
      if (snowplowConnection != null && !snowplowConnection.isClosed) snowplowConnection.close()
    }
  }

  def executeByRange(dateRange: (DateTime, DateTime)) = {
    var snowplowConnection: Connection = null
    var snowplowSatement: Statement = null

    try {
      snowplowConnection = Database.Snowplow.getConnection
      snowplowSatement = snowplowConnection.createStatement()

        val metricRowsToInsert = snowplowSatement.executeQuery(metricSelectionSQLQuery(dateRange))
        while (metricRowsToInsert.next) {
          insertAggregateIntoTableauDb(metricRowsToInsert)
        }

    } finally {
      if (snowplowSatement != null && !snowplowConnection.isClosed) snowplowSatement.close()
      if (snowplowConnection != null && !snowplowConnection.isClosed) snowplowConnection.close()
    }
  }


  /**
    *
    * @param metricRow
    */
  def insertAggregateIntoTableauDb(metricRow: ResultSet): Unit = {
    println(s"Inserting aggregate for day=${metricRow.getDate("date")} and trkref=${metricRow.getString("trkref")} ")
    var tableauConnection: Connection = null
    var statement:Statement = null
    try {

      tableauConnection = Database.Tableau.getConnection
      statement = tableauConnection.createStatement()

      val aggregateRowExists = numberOfRowsForDate(metricRow.getString("date"), statement) > 0

      if (aggregateRowExists) {
        statement.executeUpdate(aggregatePerTrkrefPerDayUpdateQuery(metricRow))
      } else {
        statement.executeUpdate(aggregatePerTrkrefPerDayCreationQuery(metricRow))
        statement.executeUpdate(aggregatePerTrkrefPerDayUpdateQuery(metricRow))
      }
    } finally {
      if (statement != null && !statement.isClosed) statement.close()
      if (tableauConnection != null && !tableauConnection.isClosed) tableauConnection.close()
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
    * @param dateRange
    * @return
    */
  def metricSelectionSQLQuery(dateRange: (DateTime, DateTime)): String

  /**
    *
    * @param date
    * @param statement
    * @return
    */
  private def numberOfRowsForDate(date: String, statement: Statement) = {
    val resultSet = statement.executeQuery(
      s"SELECT count(*) FROM ${Database.OverviewDashboardDataTableName} WHERE date_day = ${date}")
    resultSet.next
    resultSet.getLong("count")
  }
}
