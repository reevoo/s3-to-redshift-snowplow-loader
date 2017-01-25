package com.reevoo.snowplow.metrics

import com.github.nscala_time.time.Imports.DateTime
import java.sql.ResultSet


object TotalSessionTimeWithClickAndWithNotClickPerTrkrefPerDay extends DateRangeMetric {

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
       |	from atomic.mark_events
       |	where trkref is not null
       |	and derived_tstamp BETWEEN '${dateRange._1}' and '${dateRange._2}'
       |	group by 1, 2, 3, 4, 5
       |)
       |where session_duration > 0
       |group by 1, 2, 3, 4
       |order by 4, 1""".stripMargin
  }

  def aggregatePerTrkrefPerDayUpdateQuery(metricRow: ResultSet) = {
    s"""
       | UPDATE $AggregatesTableName
       | SET clicked_time_on_site=nvl(clicked_time_on_site, 0) + ${metricRow.getLong("clicked_time_on_site")},
       |     didnt_click_time_on_site=nvl(didnt_click_time_on_site, 0) + ${metricRow.getLong("didnt_click_time_on_site")}
       | WHERE trkref='${metricRow.getString("trkref")}'
       | AND date_day='${metricRow.getDate("date")}'""".stripMargin
  }

}
