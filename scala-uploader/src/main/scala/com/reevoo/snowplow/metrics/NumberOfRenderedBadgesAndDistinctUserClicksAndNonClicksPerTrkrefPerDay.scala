package com.reevoo.snowplow.metrics

import com.github.nscala_time.time.Imports.DateTime
import java.sql.ResultSet


object NumberOfRenderedBadgesAndDistinctUserClicksAndNonClicksPerTrkrefPerDay extends DateRangeMetric {

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
       |FROM atomic.mark_events
       |WHERE trkref IS NOT NULL
       |AND derived_tstamp BETWEEN '${dateRange._1}' and '${dateRange._2}'
       |AND event_type in ('rendered','clicked')
       |GROUP BY 1, 2, 3, 4, 5
       |) GROUP BY 1,2,3,4
       |
      """.stripMargin
  }


  def aggregatePerTrkrefPerDayUpdateQuery(metricRow: ResultSet) = {
    s"""
       | UPDATE $AggregatesTableName
       | SET clicked=nvl(clicked, 0) + ${metricRow.getInt("clicked")},
       |     didnt_click=nvl(didnt_click, 0) + ${metricRow.getInt("didnt_click")},
       |     rendered=nvl(rendered, 0) + ${metricRow.getInt("rendered")}
       | WHERE trkref='${metricRow.getString("trkref")}'
       | AND date_day='${metricRow.getDate("date")}'""".stripMargin
  }


}
