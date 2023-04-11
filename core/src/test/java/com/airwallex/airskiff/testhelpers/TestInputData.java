package com.airwallex.airskiff.testhelpers;

import org.apache.commons.lang3.builder.CompareToBuilder;

import java.io.Serializable;

public class TestInputData implements Comparable<TestInputData>, Serializable {
  public Integer a;
  public String b;

  public TestInputData() {
  }

  public TestInputData(Integer x) {
    a = x;
    b = x.toString();
  }

  public TestInputData(Integer x, String k) {
    a = x;
    b = k;
  }

  @Override
  public int compareTo(TestInputData that) {
    return CompareToBuilder.reflectionCompare(this, that);
  }

  @Override
  public String toString() {
    return "TestInputData{" + "a=" + a + ", b='" + b + '\'' + '}';
  }

  public Integer getA() {
    return a;
  }

  public void setA(Integer a) {
    this.a = a;
  }

  public String getB() {
    return b;
  }

  public void setB(String b) {
    this.b = b;
  }

  @Override
  public boolean equals(Object o) {
    if (o instanceof TestInputData) {
      var that = (TestInputData) o;
      return a.equals(that.a) && b.equals(that.b);
    }
    return false;
  }
}
