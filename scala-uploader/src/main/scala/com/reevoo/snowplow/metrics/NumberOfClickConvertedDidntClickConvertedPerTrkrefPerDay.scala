package com.reevoo.snowplow.metrics

import com.reevoo.snowplow.Database

import com.github.nscala_time.time.Imports.DateTime

import java.sql.ResultSet


/**
  * Calculates the "clicked_converted" and "didnt_click_converted" aggregate values for a date range.
  *
  * For every combination of date in the date range and distinct trkref, it will calculate the number of
  * clicked_converted and didnt_click_converted.
  *
  * The "clicked_converted" value is the number of total users per trkref per day, that purchased a product,
  * or were propense to buy a product, and which clicked on the product reviews in the previous 30 days to purchase.
  *
  * The "didnt_click_converted" value is the number of total users per trkref per day, that purchased a product,
  * or were propense to purchase a product, and which didn't click on the product reviews in the previous 30 days.
  */
object NumberOfClickConvertedDidntClickConvertedPerTrkrefPerDay extends DateRangeMetric {

  /**
    * Builds the SQL query that calculates the "clicked_converted" and "didnt_click_converted" values for the
    * specified date range.
    *
    * @param dateRange The date range for which to calculate the aggregate values.
    *
    * @return The SQL query string.
    */
  def metricSelectionSQLQuery(dateRange: (DateTime, DateTime)) = {
    s"""
       |select to_date(DATE_TRUNC('day', purchased.derived_tstamp), 'YYYY-MM-DD') as date,
       |       to_date(date_trunc('week', purchased.derived_tstamp::timestamp), 'YYYY-MM-DD') as date_week,
       |       to_date(date_trunc('month', purchased.derived_tstamp::timestamp), 'YYYY-MM-DD') as date_month,
       |       purchased.trkref,
       |       purchased.domain_userid,
       |       listagg(json_extract_path_text(purchased.reviewable_context, 'sku'), ' , ') as purchased_skus,
       |       listagg(clicked_skus, ',') as clicked_skus
       |from ${Database.MarkEventsStagingTableName} purchased
       |join (
       |select to_date(DATE_TRUNC('day', derived_tstamp), 'YYYY-MM-DD') as date,
       |       to_date(date_trunc('week', derived_tstamp::timestamp), 'YYYY-MM-DD') as date_week,
       |       to_date(date_trunc('month', derived_tstamp::timestamp), 'YYYY-MM-DD') as date_month,
       |       trkref,
       |       domain_userid,
       |       listagg(json_extract_path_text(reviewable_context, 'sku'), ' , ') as clicked_skus
       |from ${Database.MarkEventsStagingTableName}
       |  where trkref is not null
       |  and event_type in ('clicked')
       |  and derived_tstamp between dateadd(day,-30,'${dateRange._1}') and '${dateRange._2}'
       |  group by 1, 2, 3, 4, 5
       |) clicked on clicked.domain_userid = purchased.domain_userid
       |  where purchased.trkref is not null
       |  and purchased.event_type in ('purchase', 'propensity_to_buy')
       |  and purchased.derived_tstamp between '${dateRange._1}' and '${dateRange._2}'
       |  group by 1, 2, 3, 4, 5
      """.stripMargin
  }

  /**
    * Builds the SQL query to update the row in  tableau database that holds the aggregate values for the specific
    * combination of date and trkref with the calculated "clicked_converted" and "dindt_click_converted" values.
    *
    * @param metricRow Object with all the calculated aggregate values for the specified combination of date and trkref.
    *
    * @return The update SQL query string.
    */
  def aggregatePerTrkrefPerDayUpdateQuery(metricRow: ResultSet) = {
    val purchasedSkus = sanitizeSkuList(metricRow.getString("purchased_skus"))
    val clickedSkus = sanitizeSkuList(metricRow.getString("clicked_skus"))
    val columnToUpdate = if (purchasedSkus.intersect(clickedSkus).isEmpty) "didnt_click_converted" else "clicked_converted"

    s"""
       | UPDATE ${Database.OverviewDashboardDataTableName} set $columnToUpdate=(nvl($columnToUpdate, 0) + 1)
       | WHERE trkref='${metricRow.getString("trkref")}'
       | AND date_day='${metricRow.getDate("date")}'
      """.stripMargin
  }

  /**
    * Sometimes clients send us the skus in the tracking events enclosed in brackets and quoutes,
    * this method eliminates those.
    *
    * @param skuList String with the coman separated list of skus to sanitize
    * @return Array of skus where each one of them have been stripped of any empty spaces at the beggining and
    *         end of the sku plus any brackets or quotes at the beginning and end of the skus.
    */
  private def sanitizeSkuList(skuList: String) = {
    if (skuList != null)
      skuList.split(",").map(_.trim).map(_.replaceAll("^([\\[{\"'])+|([\\]}\"'])+$",""))
    else Array[String]()
  }

}

