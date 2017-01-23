package com.reevoo.snowplow.actions

import com.reevoo.snowplow.Database
import com.github.nscala_time.time.Imports.DateTime
import java.sql.{ Connection, Statement }

object GetMaxAndMinDateIntervalFromDBTable {


  def execute(database: Database, tableName: String, dateColumn: String) = {
    var connection: Connection = null
    var statement: Statement = null

    try {
      connection = database.getConnection
      statement = connection.createStatement
      val resultSet = statement.executeQuery(this.query(tableName, dateColumn))
      resultSet.next
      (new DateTime(resultSet.getTimestamp("min")), new DateTime(resultSet.getTimestamp("max")))

    } finally {
      if (statement != null) statement.close()
      if (connection != null) connection.close()
    }
  }

  private def query(tableName: String, dateColumn: String) = {
    s"select MIN($dateColumn), MAX($dateColumn) from $tableName"
  }

}
