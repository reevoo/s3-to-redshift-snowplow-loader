package com.reevoo.snowplow.redshift.queries

import com.reevoo.snowplow.RedshiftService

object MaxMinDateIntervalQuery {


  def execute(tableName: String, dateColumn: String) = {
    val snowplowDatabase = RedshiftService.snowplowDatabase
    val connection = snowplowDatabase.getConnection
    try {
      val resultSet = snowplowDatabase.executeQuery(this.query(tableName, dateColumn), connection)
      resultSet.next
      (resultSet.getTimestamp("min"), resultSet.getTimestamp("max"))

    } finally {
      connection.close
    }
  }

  private def query(tableName: String, dateColumn: String) = {
    s"""
       |    select MIN(${dateColumn}), MAX(${dateColumn})
       |    from ${tableName}
       |
      """.stripMargin
  }

}
