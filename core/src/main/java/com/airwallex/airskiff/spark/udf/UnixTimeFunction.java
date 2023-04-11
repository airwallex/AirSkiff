package com.airwallex.airskiff.spark.udf;

import org.apache.spark.sql.api.java.UDF2;

import java.time.Instant;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;

public class UnixTimeFunction implements UDF2<Long, String, String> {

  @Override
  public String call(Long tsInMillis, String format) throws Exception {
    return Instant.ofEpochMilli(tsInMillis).atZone(ZoneOffset.UTC).format(DateTimeFormatter.ofPattern(format));
  }
}

