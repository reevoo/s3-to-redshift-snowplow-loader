package com.reevoo.snowplow.metrics

import com.reevoo.snowplow.Database

import com.github.nscala_time.time.Imports.DateTime

import java.sql.ResultSet

/**
  * Calculates the "clicked", "didnt_click" and "rendered" aggregate values for a date range.
  *
  * For every combination of date in the date range and distinct trkref, it will calculate the number of
  * "clicked", "didnt_click" and "rendered".
  *
  * The "rendered" is the number of total unique users per trkref per day, that had badges rendered in the pages
  * that they visited.
  *
  * The "clicked" value is the number of total unique users per trkref per day, that clicked in any of the
  * badges that were rendered in the pages the visited.
  *
  * The "didnt_click" value is the number of total unique users per trkref per day, that didn't click on any of
  * the badges that were rendered int he pages they visited.
  */
object NumberOfRenderedBadgesAndDistinctUserClicksAndNonClicksPerTrkrefPerDay extends DateRangeMetric {

  /**
    * Builds the SQL query that calculates the "rendered", "clicked" and "didnt_click" values for the
    * specified date range.
    *
    * @param dateRange The date range for which to calculate the aggregate values.
    *
    * @return The SQL query string.
    */
  def metricSelectionSQLQuery(dateRange: (DateTime, DateTime)) = {
    s"""
       |SELECT date,
       |       date_week,
       |       date_month,
       |       trkref,
       |       SUM(CASE WHEN event_type = 'clicked' THEN number_of_events ELSE 0 END) AS clicked,
       |       SUM(CASE WHEN event_type = 'rendered' THEN number_of_events ELSE 0 END) AS rendered,
       |       (SUM(CASE WHEN event_type = 'rendered' THEN number_of_events ELSE 0 END) - SUM(CASE WHEN event_type = 'clicked' THEN number_of_events ELSE 0 END)) as didnt_click
       |FROM (
       |	SELECT to_date(DATE_TRUNC('day', derived_tstamp), 'YYYY-MM-DD') AS date,
       |       to_date(date_trunc('week', derived_tstamp::timestamp), 'YYYY-MM-DD') AS date_week,
       |       to_date(date_trunc('month', derived_tstamp::timestamp), 'YYYY-MM-DD') AS date_month,
       |       trkref,
       |       event_type,
       |       COUNT(DISTINCT(domain_userid)) AS number_of_events
       |FROM ${Database.MarkEventsStagingTableName}
       |WHERE trkref IS NOT NULL
       |AND derived_tstamp BETWEEN '${dateRange._1}' and '${dateRange._2}'
       |AND event_type in ('rendered','clicked')
       |GROUP BY 1, 2, 3, 4, 5
       |) GROUP BY 1,2,3,4
       |
      """.stripMargin
  }

  /**
    * Builds the SQL query to update the row in  tableau database that holds the aggregate values for the specific
    * combination of date and trkref with the calculated "rendered", "clicked" and "dindt_click" values.
    *
    * @param metricRow Object with all the calculated aggregate values for the specified combination of date and trkref.
    *
    * @return The update SQL query string.
    */
  def aggregatePerTrkrefPerDayUpdateQuery(metricRow: ResultSet) = {
    s"""
       | UPDATE ${Database.OverviewDashboardDataTableName}
       | SET clicked=nvl(clicked, 0) + ${metricRow.getInt("clicked")},
       |     didnt_click=nvl(didnt_click, 0) + ${metricRow.getInt("didnt_click")},
       |     rendered=nvl(rendered, 0) + ${metricRow.getInt("rendered")}
       | WHERE trkref='${metricRow.getString("trkref")}'
       | AND date_day='${metricRow.getDate("date")}'""".stripMargin
  }


}
