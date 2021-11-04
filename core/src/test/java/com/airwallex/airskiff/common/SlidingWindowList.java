package com.airwallex.airskiff.common;

import com.airwallex.airskiff.common.functions.SerializableBiLambda;
import java.time.Instant;
import java.util.Deque;
import java.util.LinkedList;

/**
 * This class is not thread safe!
 */
public class SlidingWindowList<T> {
  private final Deque<T> deque;
  private final Deque<Instant> instants;

  public SlidingWindowList() {
    deque = new LinkedList<>();
    instants = new LinkedList<>();
  }

  public Iterable<T> iter() {
    return deque;
  }

  public void add(T t, Instant ts) {
    deque.add(t);
    instants.add(ts);
  }

  public void moveStartTo(Instant timeInclusive) {
    if (!deque.isEmpty()) {
      Instant now = instants.peekFirst();
      while (now.compareTo(timeInclusive) < 0) {
        deque.removeFirst();
        instants.removeFirst();
        if (deque.isEmpty()) {
          return;
        }
        now = instants.peekFirst();
      }
    }
  }

  public <OUT> OUT fold(OUT out, SerializableBiLambda<T, OUT, OUT> f) {
    for (T t : deque) {
      out = f.apply(t, out);
    }
    return out;
  }

  public long size() {
    return deque.size();
  }
}
