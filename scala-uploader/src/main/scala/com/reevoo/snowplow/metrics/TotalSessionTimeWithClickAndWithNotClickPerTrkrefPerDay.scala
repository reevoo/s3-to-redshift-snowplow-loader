package com.reevoo.snowplow.metrics

import com.reevoo.snowplow.Database

import com.github.nscala_time.time.Imports.DateTime

import java.sql.ResultSet


/**
  * Calculates the "clicked_time_on_site" and "didnt_click_time_on_site" values for a date range.
  *
  * For every combination of date in the date range and distinct trkref, it will calculate the number of
  * "clicked_time_on_site" and "didnt_click_time_on_site".
  *
  * The "clicked_time_on_site" value is the sum of the duration of all the sessions where the user clicked in at least
  * one badge. The sum values will be grouped by combination of trkref and day.
  *
  * The "didnt_click_time_on_site" value is the sum of the duration of all the sessions where the user didn't click in
  * any badge. The sum values will be grouped by combination of trkref and day.
  */
object TotalSessionTimeWithClickAndWithNotClickPerTrkrefPerDay extends DateRangeMetric {

  /**
    * Builds the SQL query that calculates the "clicked_time_on_site" and "didnt_click_time_on_site" values for the
    * specified date range.
    *
    * @param dateRange The date range for which to calculate the aggregate values.
    *
    * @return The SQL query string.
    */
  def metricSelectionSQLQuery(dateRange: (DateTime, DateTime)) = {
    s"""
       |select date,
       |       date_week,
       |       date_month,
       |       trkref,
       |       SUM(CASE WHEN clicks_count = 0 THEN session_duration ELSE 0 END) AS didnt_click_time_on_site,
       |       SUM(CASE WHEN clicks_count > 0 THEN session_duration ELSE 0 END) AS clicked_time_on_site
       |from (
       |	select to_date(DATE_TRUNC('day', derived_tstamp), 'YYYY-MM-DD') as date,
       |       to_date(date_trunc('week', derived_tstamp::timestamp), 'YYYY-MM-DD') as date_week,
       |       to_date(date_trunc('month', derived_tstamp::timestamp), 'YYYY-MM-DD') as date_month,
       |       trkref,
       |       domain_sessionid,
       |       datediff(seconds, min(derived_tstamp), max(derived_tstamp)) as session_duration,
       |       sum((CASE event_type WHEN 'clicked' THEN 1 ELSE 0 END)) as clicks_count
       |	from ${Database.MarkEventsStagingTableName}
       |	where trkref is not null
       |	and derived_tstamp BETWEEN '${dateRange._1}' and '${dateRange._2}'
       |	group by 1, 2, 3, 4, 5
       |)
       |where session_duration > 0
       |group by 1, 2, 3, 4
       |order by 4, 1""".stripMargin
  }

  /**
    * Builds the SQL query to update the row in tableau database that holds the aggregate values for the specific
    * combination of date and trkref with the calculated "clicked_time_on_site", "didnt_click_time_on_site" values.
    *
    * @param metricRow Object with all the calculated aggregate values for the specified combination of date and trkref.
    *
    * @return The update SQL query string.
    */
  def aggregatePerTrkrefPerDayUpdateQuery(metricRow: ResultSet) = {
    s"""
       | UPDATE ${Database.OverviewDashboardDataTableName}
       | SET clicked_time_on_site=nvl(clicked_time_on_site, 0) + ${metricRow.getLong("clicked_time_on_site")},
       |     didnt_click_time_on_site=nvl(didnt_click_time_on_site, 0) + ${metricRow.getLong("didnt_click_time_on_site")}
       | WHERE trkref='${metricRow.getString("trkref")}'
       | AND date_day='${metricRow.getDate("date")}'""".stripMargin
  }

}
