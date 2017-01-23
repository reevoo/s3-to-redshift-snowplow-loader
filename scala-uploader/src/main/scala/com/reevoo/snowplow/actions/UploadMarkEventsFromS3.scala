package com.reevoo.snowplow.actions

import com.github.nscala_time.time.Imports._
import com.reevoo.snowplow.{Database, S3Client}
import java.sql.{Connection, Statement}

object UploadMarkEventsFromS3 {

  private final val EventsFolderToTableName = Map(
    "events" -> Database.RootEventsTemporalTableName,
    "com_reevoo_badge_event_1" -> Database.BadgeEventsTemporalTableName,
    "com_reevoo_conversion_event_1" -> Database.ConversionEventsTemporalTableName
  )

  def execute(date: DateTime): (DateTime, DateTime) = {
    val dateFormatter = DateTimeFormat.forPattern("yyyy-MM-dd")
    val s3Service = new S3Client()
    var connection: Connection = null
    var statement: Statement = null

    try {
      connection = Database.Snowplow.getConnection
      statement = connection.createStatement()

      EventsFolderToTableName.keys.foreach(folderName => {
        val s3Urls = s3Service.getListOfFolders(folderName, dateFormatter.print(date))
        s3Urls.foreach(s3Url => {
          println(s"Copying to table ${EventsFolderToTableName(folderName)} from endpoint $s3Url")
          statement.executeUpdate(copyQuery(EventsFolderToTableName(folderName), s3Url))
        })
      })

      GetMaxAndMinDateIntervalFromDBTable.execute(
        database = Database.Snowplow,
        tableName = Database.RootEventsTemporalTableName,
        dateColumn = "collector_tstamp"
      )
    } finally {
      if (statement != null) statement.close()
      if (connection != null) connection.close()
    }
  }

  def copyQuery(tableName: String, s3Endpoint: String) = {
    s"COPY $tableName FROM '$s3Endpoint' " +
      "WITH CREDENTIALS " +
      s"'aws_access_key_id=${sys.env("SNOWPLOW_AWS_ACCESS_KEY_ID")};" +
      s"aws_secret_access_key=${sys.env("SNOWPLOW_AWS_SECRET_ACCESS_KEY")}' " +
      "GZIP REMOVEQUOTES ESCAPE TRUNCATECOLUMNS DATEFORMAT 'auto' MAXERROR 100;"
  }

}
