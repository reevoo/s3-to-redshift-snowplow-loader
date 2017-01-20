package com.reevoo.snowplow.redshift.queries.metrics

import com.github.nscala_time.time.Imports._
import com.reevoo.snowplow.RedshiftService
import com.reevoo.snowplow.redshift.queries.TotalRowCount
import org.joda.time.Days


/**
  * Created by jesuslara on 1/19/17.
  */
object NumberOfClickConvertedDidntClickConvertedPerTrkrefPerDay {

  final val DateFormatter = DateTimeFormat.forPattern("yyyy-MM-dd")



  def execute(dateRange: Tuple2[DateTime, DateTime]) = {
    val snowplowDatabase = RedshiftService.snowplowDatabase


    val connection = snowplowDatabase.getConnection
    try {

      val tableauDatabase = RedshiftService.tableauDatabase
      val tableauConnection = tableauDatabase.getConnection

      (0 to Days.daysBetween(dateRange._1, dateRange._2).getDays()).map(dateRange._1.plusDays(_)).foreach(date => {
        val resultSet = snowplowDatabase.executeQuery(this.query(date), connection)

        while (resultSet.next) {
          insertAggregatesIntoTableauDb(resultSet, tableauDatabase, tableauConnection)
        }
      })

    } finally {
      connection.close
    }
  }


  private def query(date: DateTime) = {
    s"""
       |select to_date(DATE_TRUNC('day', purchased.derived_tstamp), 'YYYY-MM-DD') as date,
       |       to_date(date_trunc('week', purchased.derived_tstamp::timestamp), 'YYYY-MM-DD') as date_week,
       |       to_date(date_trunc('month', purchased.derived_tstamp::timestamp), 'YYYY-MM-DD') as date_month,
       |       purchased.trkref,
       |       purchased.domain_userid,
       |       listagg(json_extract_path_text(purchased.reviewable_context, 'sku'), ' , ') as purchased_skus,
       |       listagg(clicked_skus, ',') as clicked_skus
       |from atomic.mark_events purchased
       |join (
       |select to_date(DATE_TRUNC('day', derived_tstamp), 'YYYY-MM-DD') as date,
       |       to_date(date_trunc('week', derived_tstamp::timestamp), 'YYYY-MM-DD') as date_week,
       |       to_date(date_trunc('month', derived_tstamp::timestamp), 'YYYY-MM-DD') as date_month,
       |       trkref,
       |       domain_userid,
       |       listagg(json_extract_path_text(reviewable_context, 'sku'), ' , ') as clicked_skus
       |from atomic.mark_events
       |  where trkref is not null
       |  and event_type in ('clicked')
       |  and derived_tstamp between dateadd(day,-30,'${date}') and '${date}'
       |  group by 1, 2, 3, 4, 5
       |) clicked on clicked.domain_userid = purchased.domain_userid
       |  where purchased.trkref is not null
       |  and purchased.event_type in ('purchase', 'propensity_to_buy')
       |  and purchased.derived_tstamp between '${date}' and '${DateFormatter.print(date)} 23:59:59'
       |  group by 1, 2, 3, 4, 5
      """.stripMargin
  }


  private def insertAggregatesIntoTableauDb(resultSet: java.sql.ResultSet, database: RedshiftService, connection: java.sql.Connection) = {

    val purchasedSkus = sanitizeSkuList(resultSet.getString("purchased_skus"))
    val clickedSkus = sanitizeSkuList(resultSet.getString("clicked_skus"))
    val columnToUpdate = if (purchasedSkus.intersect(clickedSkus).isEmpty) "didnt_click_converted" else "clicked_converted"

    val insertQuery =
      s"""
         | INSERT INTO overview_dashboard_data_testing ("trkref", "date_day", "date_week", "date_month")
         | VALUES (
         | '${resultSet.getString("trkref")}',
         | '${resultSet.getDate("date")}',
         | '${resultSet.getDate("date_week")}',
         | '${resultSet.getDate("date_month")}'
         | )
                        """.stripMargin


    val updateQuery =
      s"""
         | UPDATE overview_dashboard_data_testing set ${columnToUpdate}=(nvl(${columnToUpdate}, 0) + 1)
         | WHERE trkref='${resultSet.getString("trkref")}'
         | AND date_day='${resultSet.getDate("date")}'
      """.stripMargin


    val rowExists = TotalRowCount.execute(
      database,
      connection,
      "overview_dashboard_data_testing",
      Map("trkref" -> resultSet.getString("trkref"), "date_day" -> resultSet.getString("date"))) > 0


    if (rowExists) {
      println(updateQuery)
      database.executeUpdate(updateQuery, connection)
    } else {
      println(insertQuery)
      database.executeUpdate(insertQuery, connection)
      database.executeUpdate(updateQuery, connection)
    }

  }


  private def sanitizeSkuList(skuList: String) = {
    //  we will need to sanitize the list of skus to remove stuff that does not need to be there
    //  individualPurchasedSkus.map(_.trim).map(sku => if (sku.startsWith("[")) sku.drop(2) else sku).map(sku => if (sku.endsWith("]")) sku.dropRight(2) else sku)
    if (skuList != null)
      skuList.split(",").map(_.trim)
    else Array[String]()
  }

}

