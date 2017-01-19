package com.reevoo.snowplow

import java.util.Properties
import java.sql.DriverManager

class RedshiftService(val dbUrl: String, val connectionProperties: Properties) {

  def getConnection = {
    Class.forName(connectionProperties.get("driver").asInstanceOf[String])
    DriverManager.getConnection(dbUrl, connectionProperties)
  }


  def executeQuery(query: String, connection: java.sql.Connection) = {
    val statement = connection.createStatement()
    statement.executeQuery(query)
  }

  def executeUpdate(query: String, connection: java.sql.Connection) = {
    val statement = connection.createStatement()
    statement.executeUpdate(query)
  }


  def uploadToTable(tableName: String, s3Endpoint: String) = {
    println(s"Copying to table ${tableName} from endpoint ${s3Endpoint}")
    val connection = getConnection
    val statement = connection.createStatement()

    statement.executeUpdate(
      s"COPY ${tableName} FROM '${s3Endpoint}' " +
        "WITH CREDENTIALS " +
        s"'aws_access_key_id=${sys.env("SNOWPLOW_AWS_ACCESS_KEY_ID")};" +
        s"aws_secret_access_key=${sys.env("SNOWPLOW_AWS_SECRET_ACCESS_KEY")}' " +
        "GZIP REMOVEQUOTES ESCAPE TRUNCATECOLUMNS DATEFORMAT 'auto' MAXERROR 100;")

    statement.close()
  }


}


object RedshiftService {

  private final val DriverClass = "com.amazon.redshift.jdbc41.Driver"

  def snowplowDatabase = {
    val connectionProperties = new Properties()
    connectionProperties.setProperty("driver", DriverClass)
    connectionProperties.setProperty("user", sys.env("TARGET_SNOWPLOW_REDSHIFT_DB_USER"))
    connectionProperties.setProperty("password", sys.env("TARGET_SNOWPLOW_REDSHIFT_DB_PASSWORD"))
    new RedshiftService(sys.env("TARGET_SNOWPLOW_REDSHIFT_DB_URL"), connectionProperties)
  }

  def tableauDatabase = {
    val connectionProperties = new Properties()
    connectionProperties.setProperty("driver", DriverClass)
    connectionProperties.setProperty("user", sys.env("TARGET_TABLEAU_REDSHIFT_DB_USER"))
    connectionProperties.setProperty("password", sys.env("TARGET_TABLEAU_REDSHIFT_DB_PASSWORD"))
    new RedshiftService(sys.env("TARGET_TABLEAU_REDSHIFT_DB_URL"), connectionProperties)
  }

}
