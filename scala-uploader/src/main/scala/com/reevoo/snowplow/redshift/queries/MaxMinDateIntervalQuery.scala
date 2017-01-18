package com.reevoo.snowplow.redshift.queries

import com.reevoo.snowplow.RedshiftService

object MaxMinDateIntervalQuery {


  def execute(tableName: String, dateColumn: String) = {
    val redshiftService = new RedshiftService
    val connection = redshiftService.getConnection
    try {
      val resultSet = redshiftService.executeQuery(this.query(tableName, dateColumn), connection)
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
