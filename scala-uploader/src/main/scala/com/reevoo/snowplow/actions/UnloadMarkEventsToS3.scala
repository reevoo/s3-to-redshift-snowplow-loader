package com.reevoo.snowplow.actions

import com.reevoo.snowplow.Database
import com.reevoo.snowplow.TimeUtils._

import com.github.nscala_time.time.Imports.DateTime

import java.sql.{Connection, Statement}


/**
  * Unloads events from a table in Snowplow's Redshift database down to the following bucket in S3
  *
  *   - s3://snowplow-reevoo-unload/mark_events/
  *
  * All the events offloaded to S3 will also be deleted from the table.
  *
  * The evens downloaded will be all the available ones in the specified table, which collection data is older than
  * 60 days before the specified date in the "execute" method. We keep the most recent 60 days as we need that amount of
  * data to always remain in the table to be able to calculate some of the metrics that will need to look back at a
  * number of the previous days when running their calculations.
  *
  *
  * The data offloaded to S3 will be partitioned in folders named by date (each date folder will contain only events
  * where the collection date happened on that date).
  *
  * Note that he following environment variables will need to be set to the appropriate values when running the
  * application in order for it to be able to connect to the Redshift database and the S3 bucket:
  *
  *   - SNOWPLOW_AWS_ACCESS_KEY_ID
  *   - SNOWPLOW_AWS_SECRET_ACCESS_KEY
  *   - TARGET_SNOWPLOW_REDSHIFT_DB_URL
  *   - TARGET_SNOWPLOW_REDSHIFT_DB_USER
  *   - TARGET_SNOWPLOW_REDSHIFT_DB_PASSWORD
  *
  */
object UnloadMarkEventsToS3 {

  final val DaysToKeepInRedshift = 60

  /**
    * Triggers the offloading of data from the specified table in Redshift down to the S3 bucket, and the deletion
    * of those offloaded events from the table. The events offloaded will be all the available ones which are older
    * than 30 days from the specified threshold date.
    *
    * @param tableName Table name from where to offload and delete the events.
    * @param thresholdDate The threshold date, events older than 30 days from this date will be offloaded.
    */
  def execute(tableName: String, thresholdDate: DateTime):Unit = {
    var connection: Connection = null
    try {
      connection = Database.Snowplow.getConnection

      val (oldestEventDateInTable, _) = GetMinAndMaxDateIntervalFromDBTable.execute(
        connection, Database.MarkEventsStagingTableName, "collector_tstamp")

      listOfDaysBetween(oldestEventDateInTable, thresholdDate.minusDays(DaysToKeepInRedshift))
        .map(oldestEventDateInTable.plusDays).foreach(date => {
        if (areThereEventsForDate(tableName, date, connection)) {
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

  /**
    * Builds the SQL query that needs to be run to unload all the events from the specified table in Redshift to S3.
    *
    * Note that the method relies of the following two environment variables being available with the key
    * and secret values of an account with permission to connect to the AWS S3 bucket:
    *
    *   - SNOWPLOW_AWS_ACCESS_KEY_ID
    *   - SNOWPLOW_AWS_SECRET_ACCESS_KEY
    *
    * @param tableName Table name from where to download the events.
    * @param date Collection date of the events that will be downloaded.
    *
    * @return The SQL query string.
    */
  private def unloadQuery(tableName: String, date: String) = {
    s"""
      | UNLOAD ('SELECT * FROM $tableName WHERE collector_tstamp BETWEEN \\'$date\\' and \\'$date 23:59:59\\' ORDER BY collector_tstamp')
      | TO 's3://snowplow-reevoo-unload/mark_events/${date}_${System.currentTimeMillis}/'
      | WITH CREDENTIALS 'aws_access_key_id=${sys.env("SNOWPLOW_AWS_ACCESS_KEY_ID")};aws_secret_access_key=${sys.env("SNOWPLOW_AWS_SECRET_ACCESS_KEY")}'
      | GZIP ADDQUOTES ESCAPE;
    """.stripMargin
  }

  /**
    * Builds the SQL query that needs to be run to delete all the events for a particular collection date from a
    * table in Redshift.
    *
    * @param tableName Name of the redshift table from where to delete the events.
    * @param date Collection date of the events that need to be deleted.
    *
    * @return The SQL query string.
    */
  private def deleteQuery(tableName: String, date: String) = {
    s"DELETE FROM $tableName WHERE collector_tstamp BETWEEN '$date' and '$date 23:59:59'"
  }


  /**
    * Returns true if the specified Redshift table contains any events with the specified collection date, otherwise
    * returns false.
    *
    * @param tableName The table name to check whether it contains events.
    * @param date The collection date of the events we want to check for existence.
    * @param connection Connection to the Redshift database.
    *
    * @return true or false
    */
  private def areThereEventsForDate(tableName: String, date: DateTime, connection: Connection) = {
    var statement: Statement = null
    try {
      statement = connection.createStatement()
      val resultSet = statement.executeQuery(
        s"SELECT count(*) FROM $tableName WHERE collector_tstamp between '$date'and '${date.withTime(23,59,59,999)}'")
      resultSet.next
      resultSet.getLong("count") > 0
    } finally {
      if (statement != null && !statement.isClosed) statement.close()
    }
  }

}
