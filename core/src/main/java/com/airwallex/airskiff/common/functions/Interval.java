package com.airwallex.airskiff.common.functions;

import java.io.Serializable;
import java.time.Instant;

public class Interval implements Serializable {
  private final long startInMillis;
  private final long endInMillis;

  public Interval(long startImMillis, long endInMillis) {
    this.startInMillis = startImMillis;
    this.endInMillis = endInMillis;
  }

  public static Interval from(String start, String end) {
    Instant s = Instant.parse(start);
    Instant e = Instant.parse(end);
    return new Interval(s.toEpochMilli(), e.toEpochMilli());
  }

  public boolean contains(long t) {
    return t >= startInMillis && t < endInMillis;
  }
}
