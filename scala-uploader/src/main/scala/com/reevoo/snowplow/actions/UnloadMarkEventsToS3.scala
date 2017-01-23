package com.reevoo.snowplow.actions

import com.reevoo.snowplow.Database
import com.github.nscala_time.time.Imports.{ DateTime, DateTimeFormat }
import java.sql.Connection

import org.joda.time.Days


object UnloadMarkEventsToS3 {

  def execute(tableName: String, dateRange: (DateTime, DateTime)):Unit = {
    var connection: Connection = null
    try {
      connection = Database.Snowplow.getConnection

      val dateFormatter = DateTimeFormat.forPattern("yyyy-MM-dd")
      (0 to Days.daysBetween(dateRange._1, dateRange._2).getDays()).map(dateRange._1.plusDays(_)).foreach(date => {
        UpdateDBQuery.execute(connection, this.unloadQuery(tableName, dateFormatter.print(date)))
        UpdateDBQuery.execute(connection, this.deleteQuery(tableName, dateFormatter.print(date)))
        UpdateDBQuery.execute(connection, s"VACUUM $tableName;")
        UpdateDBQuery.execute(connection, s"ANALYZE $tableName;")
      })

    } finally {
      if (connection != null) connection.close()
    }
  }

  private def unloadQuery(tableName: String, date: String) = {
    s"""
      | UNLOAD ('SELECT * FROM $tableName WHERE collector_tstamp BETWEEN \\'$date\\' and \\'$date 23:59:59\\' ORDER BY collector_tstamp')
      | TO 's3://snowplow-reevoo-unload/mark_events_testing/${date}_${System.currentTimeMillis}/'
      | WITH CREDENTIALS 'aws_access_key_id=${sys.env("SNOWPLOW_AWS_ACCESS_KEY_ID")};aws_secret_access_key=${sys.env("SNOWPLOW_AWS_SECRET_ACCESS_KEY")}'
      | GZIP ADDQUOTES ESCAPE;
    """.stripMargin
  }

  private def deleteQuery(tableName: String, date: String) = {
    s"DELETE FROM $tableName WHERE collector_tstamp BETWEEN '$date' and '$date 23:59:59'"
  }

}
