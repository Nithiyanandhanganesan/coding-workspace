package com.onemarket.utils

import org.joda.time.format.DateTimeFormat
import org.joda.time.{DateTime, Period}

object DateUtil {

  def getDates(from: DateTime, to: DateTime, step: Period): List[String] = {
    val fmt = DateTimeFormat.forPattern("yyyy/MM/dd")
    Iterator.iterate(from)(_.plus(step)).takeWhile(!_.isAfter(to)).toList.map(x => fmt.print(x))
  }

}