package com.wrs.datamanagement.util;

import com.google.protobuf.Timestamp;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.Date;


public class DateUtil {

  private static final Logger logger = LoggerFactory.getLogger(DateUtil.class);
  private static final String OUT_FORMAT = "yyyyMMddHHmmssSSS";
  private static final String FORMAT_ERROR = "Date format should follow ISO standard";
  private static DateTimeFormatter oFormatter = DateTimeFormatter.ofPattern(OUT_FORMAT);

  public static String formatDateWithString(String dateStr){

    try {
      Date date = (dateStr == null) ? new Date() : Date.from(Instant.parse(dateStr));
      LocalDateTime idateTime = LocalDateTime.ofInstant(date.toInstant(), ZoneId.of("UTC"));
      String outDateString = idateTime.format(oFormatter);
      return outDateString;
    } catch (Exception e) {
      logger.error("Exception while formating date with given string - " + dateStr, e);
      return dateStr;
    }
  }

  public static String formatDateWithTimestamp(Timestamp timestamp) {

    if (timestamp == null) {
      Instant time = Instant.now();
      timestamp = Timestamp.newBuilder().setSeconds(time.getEpochSecond())
          .setNanos(time.getNano())
          .build();
    }

    LocalDateTime idateTime = LocalDateTime
        .ofEpochSecond(timestamp.getSeconds(), timestamp.getNanos(), ZoneOffset.UTC);
    String outDateString = idateTime.format(oFormatter);
    return outDateString;
  }

}
