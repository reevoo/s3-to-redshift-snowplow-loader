package com.reevoo.snowplow.metrics

import com.reevoo.snowplow.Database
import com.reevoo.snowplow.TimeUtils._
import com.github.nscala_time.time.Imports.DateTime
import java.sql.{Connection, ResultSet, Statement}

import com.typesafe.scalalogging.LazyLogging

/**
  * Trait with common functionality for all aggregate metrics that need to be run in specific date ranges.
  */
trait DateRangeMetric extends LazyLogging {

  /**
    * Executes the aggregate calculations individually for each day in the date range. Will insert the calculated
    * values in the appropriate Tableau database table.
    *
    * @param dateRange The date range for which to calculate the aggregates.
    */
  def executeByDay(dateRange: (DateTime, DateTime)) = {
    var snowplowConnection: Connection = null
    var snowplowSatement: Statement = null

    try {
      snowplowConnection = Database.Snowplow.getConnection
      snowplowSatement = snowplowConnection.createStatement()

      listOfDaysBetween(dateRange._1, dateRange._2).map(dateRange._1.plusDays).foreach(date => {
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

  /**
    * Executes the aggregate calculations in one single group query for the whole range, doesn't need to run
    * the calculations individually for every day in the range.
    *
    * @param dateRange The date range for which to calculate the aggregates.
    */
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
    * Inserts the calculate aggregate values for a specific combination of date and trkref in the appropriate
    * Tableau database table. If a row for the specific combination of date and trkref already exist in the destination
    * table, then the existing row is updated. Otherwise a new row is created for the date and trkref combination.
    *
    * @param metricRow Object containing all the calculated aggregate values for the specific day and trkref.
    */
  def insertAggregateIntoTableauDb(metricRow: ResultSet): Unit = {
    logger.info(s"Inserting aggregate for day=${metricRow.getDate("date")} and trkref=${metricRow.getString("trkref")} ")
    var tableauConnection: Connection = null
    var statement:Statement = null
    try {

      tableauConnection = Database.Tableau.getConnection
      statement = tableauConnection.createStatement()

      val aggregateRowExists = aggregateRowForDateAndTrkrefExists(
        metricRow.getString("date"), metricRow.getString("trkref"), tableauConnection)

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
    * Builds the query that will insert a new aggregates row in the relevant Tableau database table for a specific
    * combination of date and trkref.
    *
    * @param metricsRow Object with all the calculated aggregate values for the specific combination of date an trkref.
    *
    * @return The insert SQL query.
    */
  def aggregatePerTrkrefPerDayCreationQuery(metricsRow: ResultSet): String = {
    s"""
       | INSERT INTO ${Database.OverviewDashboardDataTableName} ("trkref", "date_day", "date_week", "date_month")
       | VALUES (
       | '${metricsRow.getString("trkref")}',
       | '${metricsRow.getDate("date")}',
       | '${metricsRow.getDate("date_week")}',
       | '${metricsRow.getDate("date_month")}'
       | )""".stripMargin
  }


  /**
    * Builds the query that will update the appropriate aggregates row in the appropriate tableau database table
    * with the provided calculated values for a combination of date and trkref.
    *
    * @param metricsRow Object with all the calculated aggregate values for the specific combination of date an trkref.
    *
    * @return The update SQL quwry.
    */
  def aggregatePerTrkrefPerDayUpdateQuery(metricsRow: ResultSet): String


  /**
    * Builds the query that will calculate all the aggregate values for the specified range from the data held in
    * the Snowplow database for that date range.
    *
    * @param dateRange The date range for which to calculate the aggregates.
    *
    * @return The aggregates calculation SQL query.
    */
  def metricSelectionSQLQuery(dateRange: (DateTime, DateTime)): String


  /**
    * Returns whether a row for the aggregates values for a specific combination of date and trkref already exists
    * in the tableau database.
    *
    * @param date Date to check.
    * @param trkref The trkref to check.
    * @param connection Connection to the tableau database.
    *
    * @return true if the row for the combination of date and trkref already exists; false otherwise.
    */
  private def aggregateRowForDateAndTrkrefExists(date: String, trkref: String, connection: Connection) = {
    var statement: Statement = null

    try {
      statement = connection.createStatement()
      val resultSet = statement.executeQuery(
        s"SELECT count(*) FROM ${Database.OverviewDashboardDataTableName} WHERE date_day = '$date' and trkref='${trkref}'")
      resultSet.next
      resultSet.getLong("count") > 0
    } finally {
      if (statement != null && !statement.isClosed) statement.close()
    }
  }
}
