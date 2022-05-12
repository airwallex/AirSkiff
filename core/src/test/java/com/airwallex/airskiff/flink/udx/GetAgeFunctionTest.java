package com.airwallex.airskiff.flink.udx;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class GetAgeFunctionTest {
  private final GetAgeFunction f = new GetAgeFunction();

  @Test
  public void testGetAgeWithNull() {
    Integer age = f.eval(100L, null);
    Assertions.assertEquals(100, age);
  }

  @Test
  public void testGetAgeWithMalformedDOB() {
    Integer age = f.eval(100L, "xxx");
    Assertions.assertEquals(100, age);
  }

  @Test
  public void testGetAge() {
    Integer age = f.eval(100L, "1900-01-01");
    Assertions.assertEquals(70, age);
  }

  @Test
  public void testGetNegativeAge() {
    Integer age = f.eval(100L, "2000-01-01");
    Assertions.assertEquals(0, age);
  }
}
