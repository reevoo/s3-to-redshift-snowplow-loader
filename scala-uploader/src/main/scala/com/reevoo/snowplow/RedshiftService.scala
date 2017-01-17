package com.reevoo.snowplow

import java.util.Properties
import java.sql.DriverManager

class RedshiftService(val historicEventsTableName: String) {

  private final val DbUrl = sys.env("TARGET_REDSHIFT_DB_URL")
  private final val DriverClass = "com.amazon.redshift.jdbc41.Driver"
  private final val DBConnectionProperties = {
    val props = new Properties()
    props.setProperty("driver", DriverClass)
    props.setProperty("user", sys.env("TARGET_REDSHIFT_DB_USER"))
    props.setProperty("password", sys.env("TARGET_REDSHIFT_DB_PASSWORD"))
    props
  }
  
  private def getConnection = {
    Class.forName(DriverClass)
    DriverManager.getConnection(DbUrl, DBConnectionProperties)
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
