package com.airwallex.airskiff.examples;

import org.apache.commons.lang3.builder.CompareToBuilder;

import java.io.Serializable;

public class Counter implements Serializable, Comparable<Counter> {
  public String key;

  public String getKey() {
    return key;
  }

  public void setKey(String key) {
    this.key = key;
  }

  public Long getC() {
    return c;
  }

  public void setC(Long c) {
    this.c = c;
  }

  public Long c;

  public Counter() {
  }

  public Counter(String key, Long c) {
    this.key = key;
    this.c = c;
  }

  @Override
  public String toString() {
    return "Counter(" + key + ", " + c + ")";
  }


  @Override
  public int compareTo(Counter that) {
    return CompareToBuilder.reflectionCompare(this, that);
  }
}
