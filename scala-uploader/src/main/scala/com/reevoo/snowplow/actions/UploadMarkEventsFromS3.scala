package com.reevoo.snowplow.actions

import com.reevoo.snowplow.{Database, S3Client}
import com.reevoo.snowplow.TimeUtils.{time, DateFormatter}

import org.joda.time.DateTime

/**
  * Copies events from the following three folders in the "https://snowplow-reevoo-unload.s3.amazonaws.com" AWS S3 bucket:
  *
  *     - events
  *     - com_reevoo_badge_event_1
  *     - com_reevoo_conversion_event_1
  *
  * Up into the following three tables, respectively, in Snowplow's Redshift database:
  *
  *     - root_events_uploaded_from_s3
  *     - badge_events_uploaded_from_s3
  *     - conversion_events_uploaded_from_s3
  *
  * Note that the application will know how to connect to Snowplow's Redshift database using the following
  * environment variables, which need to be correctly set up when running the application:
  *
  *     - TARGET_SNOWPLOW_REDSHIFT_DB_URL
  *     - TARGET_SNOWPLOW_REDSHIFT_DB_USER
  *     - TARGET_SNOWPLOW_REDSHIFT_DB_PASSWORD
  *
  */
object UploadMarkEventsFromS3 {

  /** Internal constant with a mapping indicating which event type folder in the S3 bucket need to be copied to which
    * table in Snowplow's Redshift database.
    */
  private final val EventTypeFolderToTableName = Map(
    "events" -> Database.RootEventsStagingTableName,
    "com_reevoo_badge_event_1" -> Database.BadgeEventsStagingTableName,
    "com_reevoo_conversion_event_1" -> Database.ConversionEventsStagingTableName
  )


  /**
    * Triggers the upload of data from the S3 folders into the Redshift tables. Note that inside each of the three
    * concerned S3 folders to upload (events, com_reevoo_badge_event_1 and com_reevoo_conversion_event_1), the data is
    * partitioned in subfolders which names are dates. This method triggers the loading of the data only for the folders
    * associated to a specified date.
    *
    * @param date The date associated to the folders which will be uploaded from S3 to Redshift.
    *
    * @return A tuple indicating the date range of events loaded into Redshift by this operation. The first element
    *         of the tuple is the collection date of the oldest event loaded, and the second element of the tuple is
    *         the collection date of the most recent event loaded. The date in a date folder is not guaranteed to
    *         contain events for that date only, it might contain events also from previous days, that's why this return
    *         type is necessary so we know the full date range loaded by the operation.
    */
  def execute(date: DateTime): (DateTime, DateTime) = {
    val s3Service = new S3Client()

    EventTypeFolderToTableName.keys.par.foreach(eventType => {
      val s3Urls = s3Service.getListOfDateFolders(eventType, DateFormatter.print(date))
      s3Urls.foreach(s3Url => {
        time(s"Copying to table [${EventTypeFolderToTableName(eventType)}] from endpoint [$s3Url]") {
          UpdateDBQuery.execute(Database.Snowplow, copyFromS3SQLQuery(EventTypeFolderToTableName(eventType), s3Url))
        }
      })
    })

    retrieveUploadedDateRange(date)
  }

  /**
    * Builds the SQL query that needs to be run to copy all the events from a specified file endpoint in S3
    * to the specified table in Redshift.
    *
    * Note that the method relies of the following two environment variables being available with the key
    * and secret values of an account with permission to connect to the AWS S3 bucket:
    *
    *   - SNOWPLOW_AWS_ACCESS_KEY_ID
    *   - SNOWPLOW_AWS_SECRET_ACCESS_KEY
    *
    * @param tableName Name of the table where the events need to be copied to.
    * @param s3Endpoint Endpoint of the data file in S3 that contains the events to copy.
    *
    * @return A string with the SQL query that needs to be run to trigger the copy.
    */
  private def copyFromS3SQLQuery(tableName: String, s3Endpoint: String) = {
    s"COPY $tableName FROM '$s3Endpoint' " +
      "WITH CREDENTIALS " +
      s"'aws_access_key_id=${sys.env("SNOWPLOW_AWS_ACCESS_KEY_ID")};" +
      s"aws_secret_access_key=${sys.env("SNOWPLOW_AWS_SECRET_ACCESS_KEY")}' " +
      "GZIP REMOVEQUOTES ESCAPE TRUNCATECOLUMNS DATEFORMAT 'auto' MAXERROR 100;"
  }

  /**
    * Returns the mininum and maximum event collection dates uploaded to the redshift database
    * as part of this class operation.
    *
    *
    * @return A tuple with the minimum and maximum event collection timestamps.
    */
  private def retrieveUploadedDateRange(date: DateTime) = {

      // Before calculating the minimum and maximum we need to filter out some events with invalid dates.
      // We didn't start collecting tracking data with snowplow until Dec 2015, so we can remove any events
      // with a date older than that. Otherwise we get some invalid dates like the following: 0016-07-30 18:02:18
      //
      // These seems to be an issue with Snowplow's framework, it doesn't happen very often but every once in
      // a blue moon an event with an invalid date like that will get in through their collection process.
      //
      UpdateDBQuery.execute(
        Database.Snowplow,
        s"""DELETE FROM ${Database.RootEventsStagingTableName} WHERE collector_tstamp <= '2015-12-01'
            | and collector_tstamp > '${date.withTime(23,59,59,999)}'""".stripMargin
      )

      GetMinAndMaxDateIntervalFromDBTable.
        execute(Database.Snowplow, Database.RootEventsStagingTableName ,"collector_tstamp")
  }

}
