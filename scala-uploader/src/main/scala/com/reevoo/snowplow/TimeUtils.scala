package com.reevoo.snowplow

import com.github.nscala_time.time.Imports._

object TimeUtils {

  final val DateFormatter = DateTimeFormat.forPattern("yyyy-MM-dd")

  final val DaysToKeepInRedshift = 60

  def time[T](str: String)(thunk: => T): T = {
    println(str)
    val startTime = new DateTime(System.currentTimeMillis)
    val result = thunk
    val endTime = new DateTime(System.currentTimeMillis)
    val period = new Period(startTime, endTime)
    println(s"Total time taken = ${period.getDays} days, ${period.getHours} hours, " +
      s"${period.getMinutes} minutes, ${period.getSeconds} seconds")
    result
  }

}
