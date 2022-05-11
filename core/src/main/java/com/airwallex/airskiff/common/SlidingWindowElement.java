package com.airwallex.airskiff.common;

import java.time.Instant;

public class SlidingWindowElement<T> {
  private Instant ts;
  private T element;

  public SlidingWindowElement(T t, Instant time) {
    element = t;
    ts = time;
  }

  public Instant ts() {
    return ts;
  }

  public T element() {
    return element;
  }
}
