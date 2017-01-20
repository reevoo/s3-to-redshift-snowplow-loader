package com.reevoo.snowplow.redshift.queries

import com.reevoo.snowplow.RedshiftService

object TotalRowCount {

  def execute(tableName: String, columnConstraints: Map[String,String]): Long = {
    val snowplowDatabase = RedshiftService.snowplowDatabase
    val connection = snowplowDatabase.getConnection
    try {
      execute(snowplowDatabase, connection, tableName, columnConstraints)

    } finally {
      connection.close
    }
  }


  def execute(database: RedshiftService, connection: java.sql.Connection, tableName: String, columnConstraints: Map[String,String]): Long = {
    val resultSet = database.executeQuery(this.query(tableName, columnConstraints), connection)
    resultSet.next
    resultSet.getLong("count")
  }

  private def query(tableName: String, columnConstraints: Map[String, String]) = {
    println(s" SELECT count(*) FROM ${tableName} ${getConstraintQuery(columnConstraints)}")
    s"""
       |    SELECT count(*) FROM ${tableName} ${getConstraintQuery(columnConstraints)}
       |
      """.stripMargin
  }

  private def getConstraintQuery(columnConstraints: Map[String, String]) = {
    if (columnConstraints.isEmpty || columnConstraints == null) ""
    else columnConstraints.foldLeft(" WHERE "){ case (constraintQuery, (columnName, columnValue)) => s"${constraintQuery} ${columnName} = '${columnValue}' AND " } + " true"
  }


}
