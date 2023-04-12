package com.airwallex.airskiff.spark.udf;

import org.apache.spark.sql.api.java.UDF2;

import java.text.SimpleDateFormat;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.TimeZone;

public class GetAgeFunction implements UDF2<Long, String, Integer> {

  @Override
  public Integer call(Long tsInMillis, String dateOfBirth) throws Exception {
    if (null == dateOfBirth) {
      return 100;
    }
    try {
      SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd");
      format.setTimeZone(TimeZone.getTimeZone("UTC"));
      int birthYear = format.parse(dateOfBirth).toInstant().atZone(ZoneOffset.UTC).getYear();
      return Math.max(Instant.ofEpochMilli(tsInMillis).atZone(ZoneOffset.UTC).getYear() - birthYear, 0);
    } catch (Exception e) {
      return 100;
    }
  }
}
