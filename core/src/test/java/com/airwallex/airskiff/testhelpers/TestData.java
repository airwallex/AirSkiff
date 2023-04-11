package com.airwallex.airskiff.testhelpers;

import java.io.Serializable;

public class TestData implements Serializable {
  public Integer a;
  public Integer b;
  public String c;

  public TestData() {

  }

  public TestData(Integer a, Integer b, String c) {
    this.a = a;
    this.b = b;
    this.c = c;
  }

  public Integer getA() {
    return a;
  }

  public void setA(Integer a) {
    this.a = a;
  }

  public Integer getB() {
    return b;
  }

  public void setB(Integer b) {
    this.b = b;
  }

  public String getC() {
    return c;
  }

  public void setC(String c) {
    this.c = c;
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
