package com.reevoo.snowplow.redshift.queries.metrics

import com.github.nscala_time.time.Imports._
import com.reevoo.snowplow.RedshiftService

object NumberOfDistinctUserNonClicksPerTrkrefPerDay {

  def execute(dateRange: Tuple2[DateTime, DateTime]) = {
    val tableauDatabase = RedshiftService.tableauDatabase
    val tableauConnection = tableauDatabase.getConnection

    try {

      tableauDatabase.executeUpdate(this.query(dateRange), tableauConnection)

    } finally {
      tableauConnection.close
    }
  }


  private def query(dateRange: Tuple2[DateTime, DateTime]) = {
    s"""
       | UPDATE overview_dashboard_data_testing set didnt_click= (rendered - nvl(clicked, 0))
       | WHERE date_day BETWEEN '${dateRange._1}' and '${dateRange._2}'
    """.stripMargin
  }

}
