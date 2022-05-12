package com.airwallex.airskiff.flink.udx;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class UnixTimeFunctionTest {
  UnixTimeFunction f = new UnixTimeFunction();

  @Test
  public void testTimeForYear() {
    String year = f.eval(100L, "yyyy");
    Assertions.assertEquals("1970", year);
  }

  @Test
  public void testTimeForYearMonth() {
    String year = f.eval(100L, "yyyy-MM");
    Assertions.assertEquals("1970-01", year);
  }

  @Test
  public void testTimeForYearMonthDay() {
    String year = f.eval(100L, "yyyy-MM-dd");
    Assertions.assertEquals("1970-01-01", year);
  }

  @Test
  public void testTimeWithException() {
    Assertions.assertThrows(NullPointerException.class, () -> f.eval(100L, null));
  }
}
