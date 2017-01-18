package com.reevoo.snowplow.redshift.queries

import com.reevoo.snowplow.RedshiftService

object TruncateTableQuery {

  def execute(tableNames: Iterable[String]):Unit = {
    this.execute(tableNames.toSeq)
  }

  def execute(tableNames: String*):Unit = {
    val redshiftService = new RedshiftService
    val connection = redshiftService.getConnection
    try {
      tableNames.foreach( tableName => {
        redshiftService.executeUpdate(this.query(tableName), connection)
      })

    } finally {
      connection.close
    }
  }


  private def query(tableName: String) = {
    s"TRUNCATE TABLE ${tableName}"
  }

}
