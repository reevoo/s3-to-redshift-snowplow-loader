package com.reevoo.snowplow.actions

import com.reevoo.snowplow.Database

import java.sql.{Connection, Statement}

/**
  * Convenience action class to execute a SQL update query (update, insert, create, copy... anything but a select) in
  * a database.
  *
  * It's here simply for convenience as it removes the boiler plate of having to open and close the connection and
  * the statement from other parts of the application.
  */
object UpdateDBQuery {

  /**
    * Trigger the execution of the query in the database.
    *
    * @param database com.reevoo.snoplow.Database instance that allows us to get connections to the database.
    * @param query SQL query string to run.
    */
  def execute(database: Database, query: String): Unit = {
    this.execute(database.getConnection, query: String)
  }

  /**
    * Triggers the execution of the query in the database. When the calling class already has a connection to the
    * database, it can reuse the connection to trigger the execution of the query by passing it as a parameter
    * of this method.
    *
    * @param connection The connection to the database in which to run the query.
    * @param query SQL query string to run.
    */
  def execute(connection: Connection, query: String): Unit = {
    var statement: Statement = null
    try {
      statement = connection.createStatement()
      statement.executeUpdate(query)

    } finally {
      if (statement != null && !statement.isClosed) statement.close()
    }
  }

}