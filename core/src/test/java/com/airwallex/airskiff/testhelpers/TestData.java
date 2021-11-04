package com.airwallex.airskiff.testhelpers;

public class TestData {
  public final Integer a;
  public final Integer b;
  public final String c;

  public TestData(Integer x0, Integer x1, String x2) {
    a = x0;
    b = x1;
    c = x2;
  }

  @Override
  public boolean equals(Object that) {
    if (that instanceof TestData) {
      TestData o2 = (TestData) that;
      return a.equals(o2.a) && b.equals(o2.b) && c.equals(o2.c);
    }
    return false;
  }
}
