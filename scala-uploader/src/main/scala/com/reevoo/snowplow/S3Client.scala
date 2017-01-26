package com.reevoo.snowplow

import com.amazonaws.services.s3.AmazonS3Client
import com.amazonaws.auth.BasicAWSCredentials
import com.amazonaws.services.s3.model.ListObjectsRequest
import scala.collection.JavaConversions._

/**
  * Helper AWS S3 client object used to connect to an S3 bucket and perform operations in it.
  * This helper class is a wrapper for the amazon provided com.amazonaws.services.s3.AmazonS3Client object.
  */
class S3Client {

  /**
    * The bucket to which this client connects.
    */
  private val BucketName = "snowplow-reevoo-unload"

  /**
    * Low level Amazon SDK provided client to connect to the bucket.
    */
  private val S3client = new AmazonS3Client(new BasicAWSCredentials(
    sys.env("SNOWPLOW_AWS_ACCESS_KEY_ID"),
    sys.env("SNOWPLOW_AWS_SECRET_ACCESS_KEY")
  ))

  /**
    * Returns the list of all the date folders associated to the specifided date (the folder names start with
    * the specified date), which are in the bucket associated to this class and inside the specified "eventTypeFolder"
    * in that bucket.
    *
    * @param eventTypeFolder The event type folder at the root of the bucket associated to this class.
    * @param date The date to which all the wanted folders are associated.
    *
    * @return The list of folders inside the the eventTypeFolder, which are associated to the specified date.
    */
  def getListOfDateFolders(eventTypeFolder: String, date: String) = {
    val listObjectsRequest = new ListObjectsRequest().
      withBucketName(BucketName).
      withPrefix(s"$eventTypeFolder/$date").
      withDelimiter("/")

    S3client.listObjects(listObjectsRequest).getCommonPrefixes.map(s"s3://$BucketName/" + _)
  }

}
