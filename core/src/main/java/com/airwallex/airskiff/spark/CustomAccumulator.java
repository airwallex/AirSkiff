package com.airwallex.airskiff.spark;

import com.airwallex.airskiff.common.functions.NamedMonoid;
import org.apache.spark.util.AccumulatorV2;

import java.io.Serializable;


public class CustomAccumulator<T> extends AccumulatorV2<T, T> {
  private NamedMonoid<T> monoid;
  private T state;

  public NamedMonoid<T> getMonoid() {
    return monoid;
  }

  public void setMonoid(NamedMonoid<T> monoid) {
    this.monoid = monoid;
  }

  public T getState() {
    return state;
  }

  public void setState(T state) {
    this.state = state;
  }

  @Override
  public boolean isZero() {
    return state == monoid.zero();
  }

  public CustomAccumulator(NamedMonoid<T> monoid, T state) {
    this.monoid = monoid;
    this.state = state;
  }

  public CustomAccumulator(NamedMonoid<T> monoid) {
    this.monoid = monoid;
  }

  public CustomAccumulator() {
  }

  @Override
  public AccumulatorV2<T, T> copy() {
    return new CustomAccumulator<>(monoid, state);
  }

  @Override
  public void reset() {
    state = monoid.zero();
  }

  @Override
  public void add(T v) {
    state = monoid.plus(state, v);
  }

  @Override
  public void merge(AccumulatorV2<T, T> other) {
    state = monoid.plus(state, other.value());
  }

  @Override
  public T value() {
    return state;
  }
}
