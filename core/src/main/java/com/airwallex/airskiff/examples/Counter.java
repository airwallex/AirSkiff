package com.airwallex.airskiff.examples;

public class Counter {
  public String key;
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
}
