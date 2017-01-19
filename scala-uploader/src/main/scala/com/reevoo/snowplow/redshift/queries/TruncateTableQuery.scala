package com.reevoo.snowplow.redshift.queries

import com.reevoo.snowplow.RedshiftService

object TruncateTableQuery {

  def execute(tableNames: Iterable[String]):Unit = {
    this.execute(tableNames.toSeq)
  }

  def execute(tableNames: String*):Unit = {
    val snowplowDatabase = RedshiftService.snowplowDatabase
    val connection = snowplowDatabase.getConnection
    try {
      tableNames.foreach( tableName => {
        snowplowDatabase.executeUpdate(this.query(tableName), connection)
      })

    } finally {
      connection.close
    }
  }


  private def query(tableName: String) = {
    s"TRUNCATE TABLE ${tableName}"
  }

}
