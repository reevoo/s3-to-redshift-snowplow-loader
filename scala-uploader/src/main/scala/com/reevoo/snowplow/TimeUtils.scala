package com.reevoo.snowplow

import com.github.nscala_time.time.Imports.{DateTime, DateTimeFormat, Period}
import com.typesafe.scalalogging.LazyLogging
import org.joda.time.Days

object TimeUtils extends LazyLogging {

  /**
    * Formatter used to convert instances of DateTime into string representations with format "yyy-MM-dd".
    */
  final val DateFormatter = DateTimeFormat.forPattern("yyyy-MM-dd")

  /**
    * Executes the provided function, but outputs measures the time taken to complete and outputs the information
    * in the logs.
    *
    * @param message Informative message that shoudl be used to provide a description of what the provided function
    *                operation is. It will be outputted to the logs.
    * @param thunk The function to be timed and executed.
    *
    * @return The result of executed the provided thunk function.
    */
  def time[T](message: String)(thunk: => T): T = {
    logger.info(message)
    val startTime = new DateTime(System.currentTimeMillis)
    val result = thunk
    val endTime = new DateTime(System.currentTimeMillis)
    val period = new Period(startTime, endTime)
    logger.info(s"Total time taken during $message is ${period.getDays} days, ${period.getHours} hours, " +
      s"${period.getMinutes} minutes, ${period.getSeconds} seconds")
    result
  }

  /**
    * Returns the latest date amonst the two provided.
    *
    * @param date1 One of the dates to compare.
    * @param date2 The other date to compare.
    *
    * @return The DateTime instance of the two provided which represents the latest date.
    */
  def latest(date1: DateTime, date2: DateTime) = {
    if (date1.compareTo(date2) > 0) date1 else date2
  }

  /**
    * Returns a list that contains a DateTime instance representing each of the dates between the provided range
    * "fromDate" to "toDate", both of them also inclusive.

    * @param fromDate The start of the date range.
    * @param toDate  The end of the date range.
    *
    * @return A list with a date element for each of the dates in the range.
    */
  def listOfDaysBetween(fromDate: DateTime, toDate: DateTime) = {
    0 to Days.daysBetween(fromDate, toDate).getDays
  }
}
