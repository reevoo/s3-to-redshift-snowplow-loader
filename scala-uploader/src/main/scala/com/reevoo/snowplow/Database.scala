package com.reevoo.snowplow

import java.util.Properties
import java.sql.{ DriverManager, Connection }


class Database(val dbUrl: String, val connectionProperties: Properties) {

  def getConnection = {
    Class.forName(connectionProperties.get("driver").asInstanceOf[String])
    DriverManager.getConnection(dbUrl, connectionProperties)
  }

}


object Database {

  private final val RedshiftDriverClass = "com.amazon.redshift.jdbc41.Driver"

  final val RootEventsStagingTableName = "atomic.root_events_upload_from_s3"
  final val BadgeEventsStagingTableName = "atomic.badge_events_uploaded_from_s3"
  final val ConversionEventsStagingTableName = "atomic.conversion_events_uploaded_from_s3"
  final val MarkEventsStagingTableName = "atomic.mark_events_from_s3"

  final val OverviewDashboardDataTableName = "overview_dashboard_date_testing"

  lazy final val Snowplow = {
    val connectionProperties = new Properties()
    connectionProperties.setProperty("driver", RedshiftDriverClass)
    connectionProperties.setProperty("user", sys.env("TARGET_SNOWPLOW_REDSHIFT_DB_USER"))
    connectionProperties.setProperty("password", sys.env("TARGET_SNOWPLOW_REDSHIFT_DB_PASSWORD"))
    new Database(sys.env("TARGET_SNOWPLOW_REDSHIFT_DB_URL"), connectionProperties)
  }

  lazy final val Tableau = {
    val connectionProperties = new Properties()
    connectionProperties.setProperty("driver", RedshiftDriverClass)
    connectionProperties.setProperty("user", sys.env("TARGET_TABLEAU_REDSHIFT_DB_USER"))
    connectionProperties.setProperty("password", sys.env("TARGET_TABLEAU_REDSHIFT_DB_PASSWORD"))
    new Database(sys.env("TARGET_TABLEAU_REDSHIFT_DB_URL"), connectionProperties)
  }



}
