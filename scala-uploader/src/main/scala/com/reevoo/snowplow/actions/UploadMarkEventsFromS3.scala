package com.reevoo.snowplow.actions

import com.reevoo.snowplow.{Database, S3Client}
import com.reevoo.snowplow.TimeUtils.{time, DateFormatter}

import java.sql.{Connection, Statement}
import org.joda.time.DateTime

/** This class is used for copying data files from AWS S3 buckets into tables in Redshift */
object UploadMarkEventsFromS3 {

  private final val EventsFolderToTableName = Map(
    "events" -> Database.RootEventsStagingTableName,
    "com_reevoo_badge_event_1" -> Database.BadgeEventsStagingTableName,
    "com_reevoo_conversion_event_1" -> Database.ConversionEventsStagingTableName
  )

  /** Uploads events from S3 folders to temporal tables in redshift
    *
    * Given the date parameter, it will find all the folders in S3 that start with the specified date, and upload
    * all the files in those folders to some associated temporal tables in redshift.
    *
    * @param date The date the folders in S3 that we want to upload start with.
    *
    * @return  A tuple with the actual range of event dates uploaded to the database, as the folders for a given
    * date are not guaranteed to contain only events for that date, they could also contain events for the previous day
    * or from several of the previous days.
    */
  def execute(date: DateTime): (DateTime, DateTime) = {
    val s3Service = new S3Client()
    var connection: Connection = null
    var statement: Statement = null


      EventsFolderToTableName.keys.par.foreach(folderName => {
        try {
          connection = Database.Snowplow.getConnection
          statement = connection.createStatement()

          val s3Urls = s3Service.getListOfFolders(folderName, DateFormatter.print(date))
          s3Urls.foreach(s3Url => {
            time(s"Copying to table ${EventsFolderToTableName(folderName)} from endpoint $s3Url") {
              statement.executeUpdate(copyFromS3SQLQuery(EventsFolderToTableName(folderName), s3Url))
            }
          })

        } finally {
          if (statement != null) statement.close()
          if (connection != null) connection.close()
        }
      })

      retrieveUploadedDateRange(statement)
  }

  /** Builds a SQL query used to copy data from an S3 file into Redshift.
    *
    * @param tableName The name of the table in Redshift where to copy the data.
    * @param s3Endpoint The file URI in S3 from where to copy the data
    *
    * @return The SQL query string.
    */
  private def copyFromS3SQLQuery(tableName: String, s3Endpoint: String) = {
    s"COPY $tableName FROM '$s3Endpoint' " +
      "WITH CREDENTIALS " +
      s"'aws_access_key_id=${sys.env("SNOWPLOW_AWS_ACCESS_KEY_ID")};" +
      s"aws_secret_access_key=${sys.env("SNOWPLOW_AWS_SECRET_ACCESS_KEY")}' " +
      "GZIP REMOVEQUOTES ESCAPE TRUNCATECOLUMNS DATEFORMAT 'auto' MAXERROR 100;"
  }

   /** Returns the mininum and maximum event dates uploaded to the database.
    *
    * @param statement Database statement through which to execute the sql query to select max and min dates.
    *
    * @return A tuple with the min and max event collector timestamp dates.
    */
  private def retrieveUploadedDateRange(statement: Statement) = {

    // make sure we delete events with invalid dates which sometimes do get in. We didn't start collecting
    // tracking data with snowplow until Dec 2015, so we can remove any events with a date older than that.
    // otherwise we get some invalid dates like the following: 0016-07-30 18:02:18
    UpdateDBQuery.execute(
      statement,
      s"DELETE FROM ${Database.RootEventsStagingTableName} WHERE collector_tstamp <= '2015-12-01'"
    )

    GetMinAndMaxDateIntervalFromDBTable.execute(
      database = Database.Snowplow,
      tableName = Database.RootEventsStagingTableName,
      dateColumn = "collector_tstamp"
    )
  }


}
