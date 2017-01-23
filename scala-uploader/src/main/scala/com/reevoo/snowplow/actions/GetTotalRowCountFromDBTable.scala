package com.reevoo.snowplow.actions

import com.reevoo.snowplow.Database
import java.sql.{Connection, Statement}

object GetTotalRowCountFromDBTable {

  def execute(database: Database, tableName: String, columnConstraints: Map[String, String]): Long = {
    var connection: Connection = null
    try {
      connection = database.getConnection
      execute(connection, tableName, columnConstraints)

    } finally {
      if (connection != null) connection.close()
    }
  }

  def execute(connection: Connection, tableName: String, columnConstraints: Map[String, String]): Long = {
    var statement: Statement = null
    try {
      statement = connection.createStatement()
      val resultSet = statement.executeQuery(this.query(tableName, columnConstraints))
      resultSet.next
      resultSet.getLong("count")
    } finally {
      if (statement != null) statement.close()
    }
  }

  private def query(tableName: String, columnConstraints: Map[String, String]) = {
    s"SELECT count(*) FROM $tableName ${getConstraintQuery(columnConstraints)}"
  }

  private def getConstraintQuery(columnConstraints: Map[String, String]) = {
    if (columnConstraints.isEmpty || columnConstraints == null) ""
    else columnConstraints.foldLeft(" WHERE ") {
      case (constraintQuery, (columnName, columnValue)) => s"$constraintQuery $columnName = '$columnValue' AND "
    } + " true"
  }

}
