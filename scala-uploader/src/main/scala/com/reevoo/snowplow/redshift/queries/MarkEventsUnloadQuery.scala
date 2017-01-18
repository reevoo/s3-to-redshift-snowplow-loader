package com.reevoo.snowplow.redshift.queries

import com.reevoo.snowplow.RedshiftService


object MarkEventsUnloadQuery {

  def execute(tableName: String, date: String):Unit = {
    val redshiftService = new RedshiftService
    val connection = redshiftService.getConnection
    try {
      redshiftService.executeUpdate(this.unloadQuery(tableName, date), connection)
      redshiftService.executeUpdate(this.deleteQuery(tableName, date), connection)
      redshiftService.executeUpdate(s"VACUUM ${tableName};", connection)
      redshiftService.executeUpdate(s"ANALYZE ${tableName};", connection)

    } finally {
      connection.close
    }
  }

  private def unloadQuery(tableName: String, date: String) = {
    s"""
      | UNLOAD ('SELECT * FROM ${tableName} WHERE collector_tstamp BETWEEN \\'${date}\\' and \\'${date} 23:59:59\\' ORDER BY collector_tstamp')
      | TO 's3://snowplow-reevoo-unload/mark_events_testing/${date}_${System.currentTimeMillis}/'
      | WITH CREDENTIALS 'aws_access_key_id=${sys.env("SNOWPLOW_AWS_ACCESS_KEY_ID")};aws_secret_access_key=${sys.env("SNOWPLOW_AWS_SECRET_ACCESS_KEY")}'
      | GZIP ADDQUOTES ESCAPE;
    """.stripMargin
  }

  private def deleteQuery(tableName: String, date: String) = {
    println(s"Deleting for date ${date}")
    s"DELETE FROM ${tableName} WHERE collector_tstamp BETWEEN '${date}' and '${date} 23:59:59'"
  }

}
