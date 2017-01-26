package com.reevoo.snowplow.actions

import com.reevoo.snowplow.Database
import com.github.nscala_time.time.Imports.DateTime
import java.sql.{ Connection, Statement }

/**
  * Gets the minimum and maxinum  date values among all the dates stored in a specific column in a specific
  * table in a database.
  */
object GetMinAndMaxDateIntervalFromDBTable {


  /**
    * Checks the specified table in the specified database and returns a tuple with the minimum and maximum values
    * for the specified date column.
    *
    * @param database The database where the table is located.
    * @param tableName The table name.
    * @param dateColumn The date column in the table.
    * @return A tuple of DateTime instances where the first element is the minimum date found, and the second
    *         element is the maximum date found.
    */
  def execute(database: Database, tableName: String, dateColumn: String): (DateTime, DateTime) = {
    var connection: Connection = null

    try {
      connection = database.getConnection
      this.execute(connection, tableName, dateColumn)

    } finally {
      if (connection != null) connection.close()
    }
  }

  /**
    * Checks the specified table usinf the specified database connecion and returns a tuple with the minimum and
    * maximum values for the specified date column.
    *
    * @param connection Connection to the database where the table is located.
    * @param tableName The table name.
    * @param dateColumn The date column in the table.
    * @return A tuple of DateTime instances where the first element is the minimum date found, and the second
    *         element is the maximum date found.
    */
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

  /**
    * Builds the SQL query to retrieve the minimum and maximum date values in the specified table and date column.
    *
    * @param tableName The table name.
    * @param dateColumn The date column in the table.
    *
    * @return SQL query string.
    */
  private def query(tableName: String, dateColumn: String) = {
    s"select MIN($dateColumn), MAX($dateColumn) from $tableName"
  }

}
