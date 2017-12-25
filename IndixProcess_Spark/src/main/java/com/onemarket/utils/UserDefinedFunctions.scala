package com.onemarket.utils

import java.nio.charset.StandardCharsets
import java.util.UUID

import org.apache.spark.sql.functions.udf

object UserDefinedFunctions {

  val dateUnanchored = raw"(\d{4})/(\d{2})/(\d{2})".r.unanchored

  def getYear = udf((str: String) => {
    str match {
      case dateUnanchored(year, month, day) => year
    }
  })

  def getMonth = udf((str: String) => {
    str match {
      case dateUnanchored(year, month, day) => month
    }
  })

  def getDay = udf((str: String) => {
    str match {
      case dateUnanchored(year, month, day) => day
    }
  })

  def getDataPartyId = udf((key: String) => {
    UUID.nameUUIDFromBytes(key.toLowerCase.getBytes(StandardCharsets.ISO_8859_1)).toString
  })

}