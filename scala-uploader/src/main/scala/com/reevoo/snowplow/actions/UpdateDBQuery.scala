package com.reevoo.snowplow.actions

import java.sql.{Connection, Statement}

import com.reevoo.snowplow.Database


object UpdateDBQuery {

  def execute(database: Database, query: String): Unit = {
    this.execute(database.getConnection, query: String)
  }

  def execute(connection: Connection, query: String): Unit = {
    var statement: Statement = null
    try {
      statement = connection.createStatement()
      execute(statement, query)

    } finally {
      if (statement != null) statement.close()
    }
  }

  def execute(statement: Statement, query: String): Unit = {
    statement.executeUpdate(query)
  }

}