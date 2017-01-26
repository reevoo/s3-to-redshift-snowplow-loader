package com.reevoo.snowplow

import java.util.Properties
import java.sql.DriverManager


/**
  * Convenience Database Connection Factory object to facilitate obtaining connection to a specific database.
  *
  * Every instance of this object will be initialized with the connection properties to a specific properties.
  *
  * After initialisation, getting connections to the database to which the instance is associated is simple case
  * of invoking the "getConnection" method on the instance.
  *
  */
class Database(val dbUrl: String, val connectionProperties: Properties) {

  /**
    * Creates a connection to the database to which this instance is associated and returns it.
    *
    * @return The connection through which we can execute queries in the database.
    */
  def getConnection = {
    Class.forName(connectionProperties.get("driver").asInstanceOf[String])
    DriverManager.getConnection(dbUrl, connectionProperties)
  }

}


/**
  * Companion object providing convenience prepopulated instances to Tableau and Snowplow Redshift databases.
  * And also a number of constants used in functionality related to these databases, like table names for example.
  */
object Database {

  private final val RedshiftDriverClass = "com.amazon.redshift.jdbc41.Driver"

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

  final val RootEventsStagingTableName = "atomic.root_events_uploaded_from_s3"
  final val BadgeEventsStagingTableName = "atomic.badge_events_uploaded_from_s3"
  final val ConversionEventsStagingTableName = "atomic.conversion_events_uploaded_from_s3"
  final val MarkEventsStagingTableName = "atomic.mark_events_from_s3"
  final val OverviewDashboardDataTableName = "overview_dashboard_data_test"

}
