package com.airwallex.airskiff.flink.udx;

import org.apache.flink.table.functions.ScalarFunction;

import java.text.SimpleDateFormat;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.TimeZone;

public class GetAgeFunction extends ScalarFunction {
  public static final String name = "GetAge";
  private static final TimeZone tz = TimeZone.getTimeZone("UTC");

  public Integer eval(Long tsInMillis, String dateOfBirth) {
    if (null == dateOfBirth) {
      return 100;
    }
    try {
      SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd");
      format.setTimeZone(tz);
      int birthYear = format.parse(dateOfBirth).toInstant().atZone(ZoneOffset.UTC).getYear();
      return Math.max(Instant.ofEpochMilli(tsInMillis).atZone(ZoneOffset.UTC).getYear() - birthYear, 0);
    } catch (Exception e) {
      return 100;
    }
  }
}
