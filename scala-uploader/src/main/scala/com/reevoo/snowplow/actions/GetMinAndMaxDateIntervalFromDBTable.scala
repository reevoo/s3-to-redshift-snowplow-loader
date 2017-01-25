package com.reevoo.snowplow.actions

import com.reevoo.snowplow.Database
import com.github.nscala_time.time.Imports.DateTime
import java.sql.{ Connection, Statement }

object GetMinAndMaxDateIntervalFromDBTable {


  def execute(database: Database, tableName: String, dateColumn: String): (DateTime, DateTime) = {
    var connection: Connection = null

    try {
      connection = database.getConnection
      this.execute(connection, tableName, dateColumn)

    } finally {
      if (connection != null) connection.close()
    }
  }

  def execute(connection: Connection, tableName: String, dateColumn: String): (DateTime, DateTime) = {
    var statement: Statement = null

    try {
      statement = connection.createStatement
      val resultSet = statement.executeQuery(this.query(tableName, dateColumn))
      resultSet.next

      (new DateTime(resultSet.getTimestamp("min")), new DateTime(resultSet.getTimestamp("max")))

    } finally {
      if (statement != null && !statement.isClosed) statement.close()
    }

  }

  private def query(tableName: String, dateColumn: String) = {
    s"select MIN($dateColumn), MAX($dateColumn) from $tableName"
  }

}
