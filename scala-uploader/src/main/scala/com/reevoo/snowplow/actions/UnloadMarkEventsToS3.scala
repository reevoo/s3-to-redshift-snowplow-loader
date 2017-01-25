package com.reevoo.snowplow.actions

import com.reevoo.snowplow.Database
import com.github.nscala_time.time.Imports.{DateTime, DateTimeFormat}
import java.sql.{Connection, Statement}

import com.reevoo.snowplow.TimeUtils.{DateFormatter, DaysToKeepInRedshift, time}
import org.joda.time.Days


/** Unload to S3 all the events which collection date are older than 30 days before the specified
  * date. We need to always keep the 30 most recent days worth of events in the table as some of the aggregate
  * calculations need to look that back at the data in order to generate the aggregate values, but anything older
  * than 30 days can safely be offloaded to S3. The offloaded data will also be deleted from the database.
  *
  * The data offloaded to S3 will be partitioned in folders by date (each date folder will contain only events where
  * the collection date happened on that date).
  *
  */
object UnloadMarkEventsToS3 {

  def execute(tableName: String, date: DateTime):Unit = {
    var connection: Connection = null
    try {
      connection = Database.Snowplow.getConnection

      val (oldestEventDate, _) = GetMinAndMaxDateIntervalFromDBTable.execute(
        connection, Database.MarkEventsStagingTableName, "collector_tstamp")

      (0 to Days.daysBetween(oldestEventDate, date.minusDays(DaysToKeepInRedshift)).getDays()).map(oldestEventDate.plusDays(_)).foreach(date => {
        if (areThereEventsForDate(date, connection)) {
          time(s"Unloading MarkEvents to S3 for date ${DateFormatter.print(date)}") {
            UpdateDBQuery.execute(connection, this.unloadQuery(tableName, DateFormatter.print(date)))
            UpdateDBQuery.execute(connection, this.deleteQuery(tableName, DateFormatter.print(date)))
          }
        }
      })
      UpdateDBQuery.execute(connection, s"VACUUM $tableName;")
      UpdateDBQuery.execute(connection, s"ANALYZE $tableName;")

    } finally {
      if (connection != null && !connection.isClosed) connection.close()
    }
  }


  private def unloadQuery(tableName: String, date: String) = {
    s"""
      | UNLOAD ('SELECT * FROM $tableName WHERE collector_tstamp BETWEEN \\'$date\\' and \\'$date 23:59:59\\' ORDER BY collector_tstamp')
      | TO 's3://snowplow-reevoo-unload/mark_events/${date}_${System.currentTimeMillis}/'
      | WITH CREDENTIALS 'aws_access_key_id=${sys.env("SNOWPLOW_AWS_ACCESS_KEY_ID")};aws_secret_access_key=${sys.env("SNOWPLOW_AWS_SECRET_ACCESS_KEY")}'
      | GZIP ADDQUOTES ESCAPE;
    """.stripMargin
  }

  private def deleteQuery(tableName: String, date: String) = {
    s"DELETE FROM $tableName WHERE collector_tstamp BETWEEN '$date' and '$date 23:59:59'"
  }

  private def areThereEventsForDate(date: DateTime, connection: Connection) = {
    var statement: Statement = null
    try {
      statement = connection.createStatement()
      val resultSet = statement.executeQuery(
        s"""SELECT count(*) FROM ${Database.MarkEventsStagingTableName} WHERE  where collector_tstamp between '$date'
            | and '${date.withTime(23,59,59,999)}'""".
          stripMargin)
      resultSet.next
      resultSet.getLong("count") > 0
    } finally {
      if (statement != null && !statement.isClosed) statement.close()
    }
  }

}
