package com.airwallex.airskiff.flink.udx;

import org.apache.flink.table.functions.ScalarFunction;

import java.time.Instant;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;

public class UnixTimeFunction extends ScalarFunction {
  public static final String name = "UnixTime";

  public String eval(Long tsInMillis, String format) {
    return Instant.ofEpochMilli(tsInMillis).atZone(ZoneOffset.UTC).format(DateTimeFormatter.ofPattern(format));
  }
}
