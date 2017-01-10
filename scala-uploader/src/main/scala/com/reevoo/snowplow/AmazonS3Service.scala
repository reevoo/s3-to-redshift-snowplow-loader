package com.reevoo.snowplow

import com.amazonaws.services.s3.AmazonS3Client
import com.amazonaws.auth.BasicAWSCredentials
import com.amazonaws.services.s3.model.ListObjectsRequest
import scala.collection.JavaConversions._


class AmazonS3Service {

  private val BucketName = "snowplow-reevoo-unload"

  private val S3client = new AmazonS3Client(new BasicAWSCredentials(
    sys.env("SNOWPLOW_AWS_ACCESS_KEY_ID"),
    sys.env("SNOWPLOW_AWS_SECRET_ACCESS_KEY")
  ));

  def getRootEventFolders(folderName: String) = {
    getListOfFolders("events", folderName)
  }

  def getBadgeEventFolders(folderName: String) = {
    getListOfFolders("com_reevoo_badge_event_1", folderName)
  }

  def getConversionEventFolders(folderName: String) = {
    getListOfFolders("com_reevoo_conversion_event_1", folderName)
  }

  private def getListOfFolders(eventsFolder: String, subfolderName: String) = {
    val listObjectsRequest = new ListObjectsRequest().
      withBucketName(BucketName).
      withPrefix(s"${eventsFolder}/${subfolderName}").
      withDelimiter("/")

    S3client.listObjects(listObjectsRequest).getCommonPrefixes().map(s"s3://${BucketName}/" + _)
  }

}
